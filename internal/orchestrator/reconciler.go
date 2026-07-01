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
	maxRetries   int
	deadlineMS   int
}

// newReconciler wires a production Reconciler backed by the provided Dispatcher.
func newReconciler(d *Dispatcher, maxRetries, deadlineMS int) *Reconciler {
	return &Reconciler{
		findStuck: func(ctx context.Context, q *db.Queries, workspaceID uuid.UUID) ([]db.WorkspaceTask, error) {
			return q.ListInProgressAndReviewingForOwner(ctx, workspaceID)
		},
		bumpAttempts: BumpReenqueueAttempts,
		setBlocked:   SetBlockedWithDetails,
		enqueue:      d.EnqueueExisting,
		maxRetries:   maxRetries,
		deadlineMS:   deadlineMS,
	}
}

// ReconcileStuckDispatches is the production entry point. It queries all
// in_progress/reviewing go-owned tasks, filters those whose dispatched_at is
// older than cfg.ExecutionDeadlineMS, and processes each one.
func ReconcileStuckDispatches(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool, d *Dispatcher) error {
	workspaceUUID, err := uuid.Parse(cfg.WorkspaceID)
	if err != nil {
		return fmt.Errorf("reconcile: parse workspace id: %w", err)
	}
	return newReconciler(d, cfg.DispatchReconcileMaxRetries, cfg.ExecutionDeadlineMS).
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
