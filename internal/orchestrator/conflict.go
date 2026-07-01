package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/tiendv89/workflow-orchestrator/internal/config"
	"github.com/tiendv89/workflow-orchestrator/internal/database/queries"
)

// Conflict state constants.
const (
	ConflictStateNone       = "none"
	ConflictStateConflicted = "conflicted"
	ConflictStateResolving  = "resolving"
	ConflictStateResolved   = "resolved"
)

// SetConflicted marks a task's conflict_state as 'conflicted'.
//
// Guard: only updates when conflict_state is NOT 'resolving' (we must not interrupt
// an in-flight rebase). If the conflict_state is already 'conflicted', the update
// is a no-op (idempotent).
//
// Returns (true, nil) when the row was updated, (false, nil) when the guard
// prevented the update (resolving in-flight), and (false, err) on DB error.
func SetConflicted(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID) (bool, error) {
	const sql = `
UPDATE workspace_tasks SET
    conflict_state = 'conflicted',
    updated_at     = now()
WHERE workspace_id    = $1
  AND task_id         = $2
  AND conflict_state != 'resolving'
  AND status NOT IN ('done', 'cancelled')
RETURNING id`

	var id uuid.UUID
	err := pool.QueryRow(ctx, sql, workspaceID, taskUUID).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("SetConflicted: %w", err)
	}
	return true, nil
}

// SetResolving atomically claims the rebase slot for a conflicted task.
// Transitions conflict_state from 'conflicted' to 'resolving' and sets dispatch-in
// columns (handle, nonce, kind='rebase'). This is the guarded rebase-dispatch
// claim — first-write-wins.
//
// Returns (true, nil) on success, (false, nil) when the guard fails (task is not
// in 'conflicted' state or another agent won the claim), and (false, err) on DB error.
func SetResolving(
	ctx context.Context,
	pool *pgxpool.Pool,
	workspaceID, taskUUID uuid.UUID,
	handle, nonce string,
) (bool, error) {
	const sql = `
UPDATE workspace_tasks SET
    conflict_state     = 'resolving',
    dispatch_handle    = $3,
    dispatch_nonce     = $4,
    dispatched_at      = now(),
    reenqueue_attempts = 0,
    dispatch_kind      = 'rebase',
    updated_at         = now()
WHERE workspace_id   = $1
  AND task_id        = $2
  AND conflict_state = 'conflicted'
  AND status NOT IN ('done', 'cancelled')
RETURNING id`

	var id uuid.UUID
	err := pool.QueryRow(ctx, sql, workspaceID, taskUUID, handle, nonce).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("SetResolving: %w", err)
	}
	return true, nil
}

// SetResolved marks a successful rebase: conflict_state 'resolving' → 'resolved'.
// Clears dispatch columns and resets rebase_attempts to 0 (episode complete).
//
// Returns (true, nil) on success, (false, nil) when the guard fails (task not
// in resolving state), and (false, err) on DB error.
func SetResolved(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID) (bool, error) {
	const sql = `
UPDATE workspace_tasks SET
    conflict_state     = 'resolved',
    rebase_attempts    = 0,
    dispatch_handle    = NULL,
    dispatch_nonce     = NULL,
    dispatched_at      = NULL,
    dispatch_kind      = NULL,
    updated_at         = now()
WHERE workspace_id   = $1
  AND task_id        = $2
  AND conflict_state = 'resolving'
RETURNING id`

	var id uuid.UUID
	err := pool.QueryRow(ctx, sql, workspaceID, taskUUID).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("SetResolved: %w", err)
	}
	return true, nil
}

// MarkRebaseRetry transitions conflict_state from 'resolving' back to 'conflicted'
// and increments rebase_attempts. Used when a rebase attempt fails but the cap
// has not been reached.
//
// Returns (true, nil) on success, (false, nil) when the guard fails, and
// (false, err) on DB error.
func MarkRebaseRetry(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID) (bool, error) {
	const sql = `
UPDATE workspace_tasks SET
    conflict_state     = 'conflicted',
    rebase_attempts    = rebase_attempts + 1,
    dispatch_handle    = NULL,
    dispatch_nonce     = NULL,
    dispatched_at      = NULL,
    dispatch_kind      = NULL,
    updated_at         = now()
WHERE workspace_id   = $1
  AND task_id        = $2
  AND conflict_state = 'resolving'
RETURNING id`

	var id uuid.UUID
	err := pool.QueryRow(ctx, sql, workspaceID, taskUUID).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("MarkRebaseRetry: %w", err)
	}
	return true, nil
}

// FindConflictedTasks returns all go-owned tasks with conflict_state='conflicted'
// for the given workspace. These are candidates for rebase dispatch.
func FindConflictedTasks(ctx context.Context, pool *pgxpool.Pool, workspaceID uuid.UUID) ([]queries.WorkspaceTask, error) {
	const sql = `
SELECT
    id, workspace_id, feature_id, feature_name, task_id, task_name, title,
    repo, status, depends_on, blocked_reason, blocked_details, branch,
    execution, pr, workspace_pr, source_path, source_hash, owner,
    dispatch_handle, dispatch_nonce, dispatched_at, reenqueue_attempts,
    dispatch_kind, review_incomplete_count, max_turns_retry_count,
    rebase_attempts, conflict_state, blocked_from_status, created_at, updated_at
FROM workspace_tasks
WHERE workspace_id  = $1
  AND owner         = 'go'
  AND conflict_state = 'conflicted'
  AND status NOT IN ('done', 'cancelled')
ORDER BY updated_at ASC`

	rows, err := pool.Query(ctx, sql, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("FindConflictedTasks: query: %w", err)
	}
	defer rows.Close()

	var tasks []queries.WorkspaceTask
	for rows.Next() {
		var t queries.WorkspaceTask
		if err := rows.Scan(
			&t.ID, &t.WorkspaceID, &t.FeatureID, &t.FeatureName,
			&t.TaskID, &t.TaskName, &t.Title,
			&t.Repo, &t.Status, &t.DependsOn, &t.BlockedReason, &t.BlockedDetails,
			&t.Branch, &t.Execution, &t.Pr, &t.WorkspacePr,
			&t.SourcePath, &t.SourceHash, &t.Owner,
			&t.DispatchHandle, &t.DispatchNonce, &t.DispatchedAt,
			&t.ReenqueueAttempts, &t.DispatchKind,
			&t.ReviewIncompleteCount, &t.MaxTurnsRetryCount,
			&t.RebaseAttempts, &t.ConflictState, &t.BlockedFromStatus,
			&t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("FindConflictedTasks: scan: %w", err)
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("FindConflictedTasks: rows: %w", err)
	}
	return tasks, nil
}

// HandleRebaseCompletion processes a completed rebase for a task.
// success=true: SetResolved; success=false: retry or block based on cap + path.
//
// Path A (in_review / change_requested / any non-review_passed): block on cap.
// Path B (review_passed): stay conflicted + log Slack TODO on cap.
func HandleRebaseCompletion(
	ctx context.Context,
	pool *pgxpool.Pool,
	workspaceID, taskUUID uuid.UUID,
	success bool,
	maxRebaseAttempts int,
) error {
	if success {
		ok, err := SetResolved(ctx, pool, workspaceID, taskUUID)
		if err != nil {
			return fmt.Errorf("HandleRebaseCompletion: SetResolved: %w", err)
		}
		if !ok {
			log.Warn().
				Str("task_id", taskUUID.String()).
				Msg("HandleRebaseCompletion: SetResolved no-op (task not in resolving state)")
		}
		return nil
	}

	// Failure path: fetch current task to read rebase_attempts + status.
	var rebaseAttempts int32
	var status string
	err := pool.QueryRow(ctx,
		`SELECT rebase_attempts, status FROM workspace_tasks
		 WHERE workspace_id = $1 AND task_id = $2`,
		workspaceID, taskUUID,
	).Scan(&rebaseAttempts, &status)
	if err != nil {
		return fmt.Errorf("HandleRebaseCompletion: fetch task: %w", err)
	}

	// +1 because MarkRebaseRetry increments rebase_attempts in the same op,
	// but we haven't called it yet. Check if the next attempt would exceed the cap.
	nextAttempts := int(rebaseAttempts) + 1

	if nextAttempts >= maxRebaseAttempts {
		// Cap reached.
		if status == "review_passed" {
			// Path B: keep task in review_passed + conflicted; Slack is a TODO.
			// We still need to clear the 'resolving' state back to 'conflicted'.
			if _, err := MarkRebaseRetry(ctx, pool, workspaceID, taskUUID); err != nil {
				return fmt.Errorf("HandleRebaseCompletion: MarkRebaseRetry (Path B cap): %w", err)
			}
			log.Warn().
				Str("task_id", taskUUID.String()).
				Int("rebase_attempts", nextAttempts).
				Msg("HandleRebaseCompletion: Path B rebase cap reached — task stays conflicted; human must resolve") // TODO(slack)
		} else {
			// Path A: block the task.
			if _, err := SetBlockedWithDetails(ctx, pool, workspaceID, taskUUID,
				"rebase_failed",
				fmt.Sprintf("rebase failed after %d attempts", nextAttempts),
				status,
			); err != nil {
				return fmt.Errorf("HandleRebaseCompletion: SetBlockedWithDetails (Path A cap): %w", err)
			}
		}
		return nil
	}

	// Below cap: retry.
	if _, err := MarkRebaseRetry(ctx, pool, workspaceID, taskUUID); err != nil {
		return fmt.Errorf("HandleRebaseCompletion: MarkRebaseRetry: %w", err)
	}
	return nil
}

// rollbackResolving transitions conflict_state from 'resolving' back to
// 'conflicted' and clears dispatch columns WITHOUT incrementing rebase_attempts.
// Used exclusively as a rollback when the broker dispatch itself fails — the
// rebase never executed, so the attempt counter must not be burned.
func rollbackResolving(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID) (bool, error) {
	const sql = `
UPDATE workspace_tasks SET
    conflict_state  = 'conflicted',
    dispatch_handle = NULL,
    dispatch_nonce  = NULL,
    dispatched_at   = NULL,
    dispatch_kind   = NULL,
    updated_at      = now()
WHERE workspace_id   = $1
  AND task_id        = $2
  AND conflict_state = 'resolving'
RETURNING id`

	var id uuid.UUID
	err := pool.QueryRow(ctx, sql, workspaceID, taskUUID).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("rollbackResolving: %w", err)
	}
	return true, nil
}

// DispatchRebase claims the rebase slot and enqueues a rebase job for a
// conflicted task. Returns (true, nil) if the claim + dispatch succeeded,
// (false, nil) if another agent already won the claim, and (false, err) on error.
func DispatchRebase(
	ctx context.Context,
	cfg *config.Config,
	pool *pgxpool.Pool,
	hs *HandleStore,
	dispatcher *Dispatcher,
	task queries.WorkspaceTask,
) (bool, error) {
	handle := uuid.New().String()
	nonce := uuid.New().String()

	// Guarded claim: conflict_state 'conflicted' → 'resolving'.
	ok, err := SetResolving(ctx, pool, task.WorkspaceID, task.TaskID, handle, nonce)
	if err != nil {
		return false, fmt.Errorf("DispatchRebase: SetResolving: %w", err)
	}
	if !ok {
		return false, nil // another agent won the claim
	}

	// Register + enqueue the rebase job with the SAME nonce just persisted by
	// SetResolving above — the executor's /callback validates against it.
	if err := dispatcher.DispatchWithNonce(ctx, cfg, task, handle, nonce, "rebase"); err != nil {
		// Rollback: clear resolving state back to conflicted WITHOUT burning the
		// rebase_attempts counter. MarkRebaseRetry would count this as a failed
		// rebase execution, but the rebase never ran — only the broker dispatch
		// failed (transient infra issue). Use rollbackResolving instead.
		if _, rbErr := rollbackResolving(ctx, pool, task.WorkspaceID, task.TaskID); rbErr != nil {
			log.Warn().Err(rbErr).
				Str("task_name", task.TaskName).
				Msg("DispatchRebase: rollback failed — task may be stuck in resolving")
		}
		return false, fmt.Errorf("DispatchRebase: dispatch: %w", err)
	}

	hs.Register(handle, HandleEntry{
		FeatureUUID: task.FeatureID,
		TaskUUID:    task.TaskID,
		FeatureName: task.FeatureName,
		TaskName:    task.TaskName,
	})
	return true, nil
}
