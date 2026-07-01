package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// GuardedTransition executes an atomic guarded UPDATE on workspace_tasks.
//
// fromStatus controls the WHERE precondition:
//   - Any non-"*" value matches tasks whose current status equals fromStatus.
//   - "*" matches any non-terminal status (i.e. WHERE status NOT IN ('done','cancelled')).
//     Terminal states are always excluded from wildcard transitions — use an explicit
//     fromStatus when you need to match a specific terminal state.
//
// extra maps column names to their SET values. []byte values are treated as
// JSONB and cast accordingly; time.Time values map to timestamptz; nil and
// typed-nil pointer values map to NULL; all other types use plain $n binding.
//
// Returns (true, nil) on a successful match, (false, nil) when the
// precondition is not met (zero rows), and (false, err) on DB error.
func GuardedTransition(
	ctx context.Context,
	pool *pgxpool.Pool,
	workspaceID, taskUUID uuid.UUID,
	fromStatus, toStatus string,
	extra map[string]any,
) (bool, error) {
	// Fixed args: $1=toStatus, $2=workspaceID, $3=taskUUID.
	args := []any{toStatus, workspaceID, taskUUID}
	setClauses := []string{"status = $1", "updated_at = now()"}
	argIdx := 4

	for col, val := range extra {
		switch v := val.(type) {
		case []byte:
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d::jsonb", col, argIdx))
			args = append(args, v)
		case time.Time:
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, argIdx))
			args = append(args, v)
		default:
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, argIdx))
			args = append(args, v)
		}
		argIdx++
	}

	where := "workspace_id = $2 AND task_id = $3"
	if fromStatus == "*" {
		// Wildcard: match any non-terminal status. This prevents late-arriving
		// failure completions from regressing already-done/cancelled tasks.
		where += " AND status NOT IN ('done','cancelled')"
	} else {
		where += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, fromStatus)
	}

	sql := fmt.Sprintf(
		`UPDATE workspace_tasks SET %s WHERE %s RETURNING id`,
		strings.Join(setClauses, ", "), where,
	)

	var id uuid.UUID
	err := pool.QueryRow(ctx, sql, args...).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("GuardedTransition: %w", err)
	}
	return true, nil
}

// dispatchInExtra returns the SET columns for a dispatch-in operation:
// sets handle, nonce, dispatched_at, kind and resets reenqueue_attempts to 0.
func dispatchInExtra(handle, nonce, kind string) map[string]any {
	return map[string]any{
		"dispatch_handle":    handle,
		"dispatch_nonce":     nonce,
		"dispatched_at":      time.Now().UTC(),
		"reenqueue_attempts": int32(0),
		"dispatch_kind":      kind,
	}
}

// dispatchOutExtra returns the SET columns for a dispatch-out operation:
// clears handle, nonce, dispatched_at, kind. reenqueue_attempts is NOT cleared —
// it resets to 0 only on the next dispatch-in.
func dispatchOutExtra() map[string]any {
	return map[string]any{
		"dispatch_handle": (*string)(nil),
		"dispatch_nonce":  (*string)(nil),
		"dispatched_at":   (*time.Time)(nil),
		"dispatch_kind":   (*string)(nil),
	}
}

// mergeExtra merges two extra maps (b values override a).
func mergeExtra(a, b map[string]any) map[string]any {
	out := make(map[string]any, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// SetInReview transitions a task from "in_progress" to "in_review" and records
// the PR URL in the pr JSONB field as {"url": prURL, "status": "open"}.
// Clears dispatch columns (dispatch-out) and resets max_turns_retry_count to 0
// (success-exit rule: counter resets on every successful impl/fix completion).
// Returns (false, nil) when the task was not in "in_progress".
func SetInReview(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID, prURL string) (bool, error) {
	pr, err := json.Marshal(map[string]string{"url": prURL, "status": "open"})
	if err != nil {
		return false, fmt.Errorf("SetInReview: marshal pr: %w", err)
	}
	extra := mergeExtra(dispatchOutExtra(), map[string]any{
		"pr":                    pr,
		"max_turns_retry_count": int32(0),
	})
	ok, err := GuardedTransition(ctx, pool, workspaceID, taskUUID, "in_progress", "in_review", extra)
	if err != nil {
		return false, fmt.Errorf("SetInReview: %w", err)
	}
	return ok, nil
}

// SetBlocked transitions a task to "blocked" from any non-terminal status and
// records reason, details, and the status the task was in before blocking
// (for cause-aware unblock resume).
func SetBlocked(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID, reason string) (bool, error) {
	return SetBlockedWithDetails(ctx, pool, workspaceID, taskUUID, reason, "", "")
}

// SetBlockedWithDetails is SetBlocked with an optional human-readable details
// string and the current status (fromStatus) to persist as blocked_from_status.
// Pass fromStatus="" to skip recording it (legacy callers).
func SetBlockedWithDetails(
	ctx context.Context,
	pool *pgxpool.Pool,
	workspaceID, taskUUID uuid.UUID,
	reason, details, fromStatus string,
) (bool, error) {
	extra := mergeExtra(dispatchOutExtra(), map[string]any{
		"blocked_reason": reason,
	})
	if details != "" {
		extra["blocked_details"] = details
	}
	if fromStatus != "" {
		extra["blocked_from_status"] = fromStatus
	}
	ok, err := GuardedTransition(ctx, pool, workspaceID, taskUUID, "*", "blocked", extra)
	if err != nil {
		return false, fmt.Errorf("SetBlocked: %w", err)
	}
	return ok, nil
}

// SetReviewing transitions a task from fromStatus ("in_review" or "review_incomplete")
// to "reviewing" and sets the dispatch-in columns atomically.
// This is the guarded reviewer-dispatch claim — first-push-wins.
func SetReviewing(
	ctx context.Context,
	pool *pgxpool.Pool,
	workspaceID, taskUUID uuid.UUID,
	fromStatus, handle, nonce string,
) (bool, error) {
	extra := dispatchInExtra(handle, nonce, "review")
	ok, err := GuardedTransition(ctx, pool, workspaceID, taskUUID, fromStatus, "reviewing", extra)
	if err != nil {
		return false, fmt.Errorf("SetReviewing: %w", err)
	}
	return ok, nil
}

// SetReviewPassed transitions a task from "reviewing" to "review_passed" and
// clears dispatch columns (dispatch-out). The task is now awaiting impl PR merge.
func SetReviewPassed(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID) (bool, error) {
	ok, err := GuardedTransition(ctx, pool, workspaceID, taskUUID, "reviewing", "review_passed", dispatchOutExtra())
	if err != nil {
		return false, fmt.Errorf("SetReviewPassed: %w", err)
	}
	return ok, nil
}

// SetChangeRequested transitions a task from "reviewing" to "change_requested" and
// clears dispatch columns. The fix loop will re-claim from change_requested.
func SetChangeRequested(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID) (bool, error) {
	ok, err := GuardedTransition(ctx, pool, workspaceID, taskUUID, "reviewing", "change_requested", dispatchOutExtra())
	if err != nil {
		return false, fmt.Errorf("SetChangeRequested: %w", err)
	}
	return ok, nil
}

// SetReviewIncomplete transitions a task from "reviewing" to "review_incomplete",
// clears dispatch columns, and increments review_incomplete_count by 1.
// The count is incremented using a separate UPDATE within the same transaction-like
// approach: the guarded transition fires only if the precondition matches.
func SetReviewIncomplete(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID) (bool, error) {
	// Increment review_incomplete_count inline in the SET clause.
	// GuardedTransition doesn't support SQL expressions, so we use a direct UPDATE.
	const sql = `
UPDATE workspace_tasks SET
    status                 = 'review_incomplete',
    review_incomplete_count = review_incomplete_count + 1,
    dispatch_handle        = NULL,
    dispatch_nonce         = NULL,
    dispatched_at          = NULL,
    dispatch_kind          = NULL,
    updated_at             = now()
WHERE workspace_id = $1
  AND task_id      = $2
  AND status       = 'reviewing'
RETURNING id`

	var id uuid.UUID
	err := pool.QueryRow(ctx, sql, workspaceID, taskUUID).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("SetReviewIncomplete: %w", err)
	}
	return true, nil
}

// SetDoneFromMergedPR transitions a task to "done" when its impl PR is
// observed merged on GitHub. This is ground-truth: the PR being merged
// supersedes any in-flight reviewer result. Accepts from any of
// "in_review", "reviewing", or "review_passed".
// In the same transaction, advances any go-owned todo tasks whose full
// dependency set is now satisfied.
func SetDoneFromMergedPR(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID) (bool, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("SetDoneFromMergedPR: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	const sql = `
UPDATE workspace_tasks SET
    status     = 'done',
    updated_at = now()
WHERE workspace_id = $1
  AND task_id      = $2
  AND status IN ('in_review', 'reviewing', 'review_passed')
RETURNING id, task_name`

	var id uuid.UUID
	var taskName string
	err = tx.QueryRow(ctx, sql, workspaceID, taskUUID).Scan(&id, &taskName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("SetDoneFromMergedPR: guarded update: %w", err)
	}

	if _, err := AutoReadyDependents(ctx, tx, workspaceID, taskName); err != nil {
		return false, fmt.Errorf("SetDoneFromMergedPR: auto-ready: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("SetDoneFromMergedPR: commit: %w", err)
	}
	return true, nil
}

// SetDone transitions a task from "in_review" to "done" and, in the same
// transaction, advances any go-owned todo tasks whose full dependency set is
// now satisfied (AutoReadyDependents).
// Returns (false, nil) when the task was not in "in_review".
//
// Deprecated: prefer SetDoneFromMergedPR which accepts additional from-statuses.
func SetDone(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID) (bool, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("SetDone: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Guarded UPDATE: fetch task_name in the same round-trip via RETURNING.
	var id uuid.UUID
	var taskName string
	err = tx.QueryRow(ctx,
		`UPDATE workspace_tasks SET status=$1, updated_at=now()
		 WHERE workspace_id=$2 AND task_id=$3 AND status=$4 RETURNING id, task_name`,
		"done", workspaceID, taskUUID, "in_review",
	).Scan(&id, &taskName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("SetDone: guarded update: %w", err)
	}

	if _, err := AutoReadyDependents(ctx, tx, workspaceID, taskName); err != nil {
		return false, fmt.Errorf("SetDone: auto-ready: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("SetDone: commit: %w", err)
	}
	return true, nil
}

// SetReadyFromMaxTurns transitions a task from "in_progress" to "ready" for a
// max-turns retry, incrementing max_turns_retry_count atomically in the same
// UPDATE. Clears dispatch columns so the next claim sees a clean slate.
// Returns (false, nil) when the task was not in "in_progress".
func SetReadyFromMaxTurns(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID) (bool, error) {
	const sql = `
UPDATE workspace_tasks SET
    status                = 'ready',
    max_turns_retry_count = max_turns_retry_count + 1,
    dispatch_handle       = NULL,
    dispatch_nonce        = NULL,
    dispatched_at         = NULL,
    dispatch_kind         = NULL,
    updated_at            = now()
WHERE workspace_id = $1
  AND task_id      = $2
  AND status       = 'in_progress'
RETURNING id`

	var id uuid.UUID
	err := pool.QueryRow(ctx, sql, workspaceID, taskUUID).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("SetReadyFromMaxTurns: %w", err)
	}
	return true, nil
}

// BumpReenqueueAttempts atomically increments reenqueue_attempts for a task and
// returns the new count. The reconciler calls this durably before re-enqueuing
// to Redis — if Redis fails the count is already committed, preventing unbounded
// retry without accounting.
func BumpReenqueueAttempts(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID) (int32, error) {
	var newCount int32
	err := pool.QueryRow(ctx,
		`UPDATE workspace_tasks
		 SET reenqueue_attempts = reenqueue_attempts + 1, updated_at = now()
		 WHERE workspace_id = $1 AND task_id = $2
		 RETURNING reenqueue_attempts`,
		workspaceID, taskUUID,
	).Scan(&newCount)
	if err != nil {
		return 0, fmt.Errorf("BumpReenqueueAttempts: %w", err)
	}
	return newCount, nil
}

// GetMaxTurnsRetryCount returns the current max_turns_retry_count for a task.
// Used by the reaper to decide between retry (in_progress→ready) and block.
func GetMaxTurnsRetryCount(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID) (int32, error) {
	var count int32
	err := pool.QueryRow(ctx,
		`SELECT max_turns_retry_count FROM workspace_tasks
		 WHERE workspace_id = $1 AND task_id = $2`,
		workspaceID, taskUUID,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("GetMaxTurnsRetryCount: %w", err)
	}
	return count, nil
}
