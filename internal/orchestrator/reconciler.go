package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/tiendv89/workflow-orchestrator/internal/config"
	db "github.com/tiendv89/workflow-orchestrator/internal/database/queries"
	gh "github.com/tiendv89/workflow-orchestrator/internal/github"
)

// Reconciler scans for stuck in_progress/reviewing dispatches and either
// re-enqueues them (using the same handle+nonce) or blocks the task after
// DISPATCH_RECONCILE_MAX_RETRIES attempts.
//
// All fields are injectable so the unit tests can run without a real DB or Redis.
type Reconciler struct {
	findStuck    func(ctx context.Context, q *db.Queries, workspaceID uuid.UUID) ([]db.WorkspaceTask, error)
	bumpAttempts func(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID) (int32, error)
	setBlocked   func(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID, reason, details, fromStatus string) (bool, error)
	enqueue      func(ctx context.Context, cfg *config.Config, task db.WorkspaceTask, handle, nonce, kind string) error
	// isMerged is an optional check: given a PR URL, returns true when the PR is
	// already merged. When non-nil, reconcileOne skips re-enqueue for reviewing
	// tasks whose PR is already merged — PollMergedPRs (later in the same cycle)
	// will mark those tasks done cleanly.
	isMerged   func(ctx context.Context, prURL string) (bool, error)
	maxRetries int
	deadlineMS int
}

// newReconciler wires a production Reconciler backed by the provided Dispatcher.
// ghClient is used to skip re-enqueueing reviewing tasks whose PR is already
// merged (nil disables the check, e.g. in unit tests that don't wire a client).
func newReconciler(d *Dispatcher, ghClient gh.PRGetter, maxRetries, deadlineMS int) *Reconciler {
	var isMerged func(ctx context.Context, prURL string) (bool, error)
	if ghClient != nil {
		isMerged = func(ctx context.Context, prURL string) (bool, error) {
			status, err := ghClient.GetPR(ctx, prURL)
			if err != nil {
				return false, err
			}
			return status.Merged, nil
		}
	}
	return &Reconciler{
		findStuck: func(ctx context.Context, q *db.Queries, workspaceID uuid.UUID) ([]db.WorkspaceTask, error) {
			return q.ListInProgressAndReviewingForOwner(ctx, workspaceID)
		},
		bumpAttempts: BumpReenqueueAttempts,
		setBlocked:   SetBlockedWithDetails,
		enqueue:      d.EnqueueExisting,
		isMerged:     isMerged,
		maxRetries:   maxRetries,
		deadlineMS:   deadlineMS,
	}
}

// ReconcileStuckDispatches is the production entry point. It queries all
// in_progress/reviewing go-owned tasks, filters those whose dispatched_at is
// older than cfg.ExecutionDeadlineMS, and processes each one.
//
// ghClient is used to skip re-enqueueing reviewing tasks whose PR is already
// merged (the same-cycle PollMergedPRs step will mark them done). Pass nil to
// disable the check (e.g. when the GitHub client is unavailable).
func ReconcileStuckDispatches(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool, d *Dispatcher, ghClient gh.PRGetter) error {
	workspaceUUID, err := uuid.Parse(cfg.WorkspaceID)
	if err != nil {
		return fmt.Errorf("reconcile: parse workspace id: %w", err)
	}
	return newReconciler(d, ghClient, cfg.DispatchReconcileMaxRetries, cfg.ExecutionDeadlineMS).
		reconcile(ctx, cfg, pool, workspaceUUID)
}

func (r *Reconciler) reconcile(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool, workspaceUUID uuid.UUID) error {
	q := db.New(pool)
	tasks, err := r.findStuck(ctx, q, workspaceUUID)
	if err != nil {
		return fmt.Errorf("reconcile: list tasks: %w", err)
	}

	deadline := time.Now().UTC().Add(-time.Duration(r.deadlineMS) * time.Millisecond)
	for _, task := range tasks {
		// Skip tasks that have no handle (not dispatched yet) or whose dispatch
		// is still within the deadline window.
		if task.DispatchHandle == nil || task.DispatchNonce == nil {
			continue
		}
		if task.DispatchedAt.Valid && !task.DispatchedAt.Time.Before(deadline) {
			continue
		}

		if err := r.reconcileOne(ctx, cfg, pool, workspaceUUID, task); err != nil {
			log.Warn().Err(err).
				Str("task", task.TaskName).
				Str("handle", *task.DispatchHandle).
				Msg("reconciler: failed to process task — skipping")
		}
	}
	return nil
}

func (r *Reconciler) reconcileOne(
	ctx context.Context,
	cfg *config.Config,
	pool *pgxpool.Pool,
	workspaceUUID uuid.UUID,
	task db.WorkspaceTask,
) error {
	handle := *task.DispatchHandle
	nonce := *task.DispatchNonce

	kind := "impl"
	if task.DispatchKind != nil && *task.DispatchKind != "" {
		kind = *task.DispatchKind
	}

	fromStatus := ""
	if task.Status != nil {
		fromStatus = *task.Status
	}

	// For reviewing tasks: skip re-enqueue when the PR is already merged.
	// PollMergedPRs (later in the same cycle) will transition the task to done.
	// GitHub API errors are non-fatal — log and continue to re-enqueue.
	if fromStatus == "reviewing" && r.isMerged != nil {
		if prURL := extractPRURL(task.Pr); prURL != "" {
			merged, err := r.isMerged(ctx, prURL)
			if err != nil {
				log.Warn().Err(err).
					Str("task", task.TaskName).
					Str("pr_url", prURL).
					Msg("reconciler: GetPR failed — proceeding with re-enqueue")
			} else if merged {
				log.Debug().
					Str("task", task.TaskName).
					Str("pr_url", prURL).
					Msg("reconciler: PR already merged — skipping re-enqueue")
				return nil
			}
		}
	}

	if task.ReenqueueAttempts >= int32(r.maxRetries) {
		details := fmt.Sprintf(
			"task stuck for >%dms; %d re-enqueue attempts exhausted",
			r.deadlineMS, r.maxRetries,
		)
		if _, err := r.setBlocked(ctx, pool, workspaceUUID, task.TaskID, "reconciler_max", details, fromStatus); err != nil {
			return fmt.Errorf("reconcileOne: setBlocked: %w", err)
		}
		log.Info().
			Str("task", task.TaskName).
			Str("handle", handle).
			Int32("attempts", task.ReenqueueAttempts).
			Msg("reconciler: task blocked after max re-enqueue attempts")
		return nil
	}

	// Increment durably before enqueuing (design requirement: "before enqueue").
	// If Redis fails afterwards, the incremented count is already committed —
	// the next reconciler cycle will pick up where this one left off.
	newCount, err := r.bumpAttempts(ctx, pool, workspaceUUID, task.TaskID)
	if err != nil {
		return fmt.Errorf("reconcileOne: bumpAttempts: %w", err)
	}

	if err := r.enqueue(ctx, cfg, task, handle, nonce, kind); err != nil {
		// Redis failure is infra-level: never roll back, never block the task.
		// The reconciler retries on the next cycle using the same handle+nonce.
		log.Warn().Err(err).
			Str("handle", handle).
			Str("task", task.TaskName).
			Int32("reenqueue_attempts", newCount).
			Msg("reconciler: redis re-enqueue failed — will retry next cycle")
		return nil
	}

	log.Info().
		Str("handle", handle).
		Str("task", task.TaskName).
		Int32("reenqueue_attempts", newCount).
		Int("max", r.maxRetries).
		Msg("reconciler: task re-enqueued with existing handle+nonce")
	return nil
}
