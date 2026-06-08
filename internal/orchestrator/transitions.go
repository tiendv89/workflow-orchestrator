package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// GuardedTransition executes an atomic guarded UPDATE on workspace_tasks.
//
// fromStatus controls the WHERE precondition:
//   - Any non-"*" value matches tasks whose current status equals fromStatus.
//   - "*" skips the status precondition (transition from any status).
//
// extra maps column names to their SET values. []byte values are treated as
// JSONB and cast accordingly; all other types use plain $n binding.
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
		default:
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, argIdx))
			args = append(args, v)
		}
		argIdx++
	}

	where := "workspace_id = $2 AND task_id = $3"
	if fromStatus != "*" {
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

// SetInReview transitions a task from "in_progress" to "in_review" and records
// the PR URL in the pr JSONB field as {"url": prURL, "status": "open"}.
// Returns (false, nil) when the task was not in "in_progress".
func SetInReview(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID, prURL string) (bool, error) {
	pr, err := json.Marshal(map[string]string{"url": prURL, "status": "open"})
	if err != nil {
		return false, fmt.Errorf("SetInReview: marshal pr: %w", err)
	}
	ok, err := GuardedTransition(ctx, pool, workspaceID, taskUUID, "in_progress", "in_review", map[string]any{"pr": pr})
	if err != nil {
		return false, fmt.Errorf("SetInReview: %w", err)
	}
	return ok, nil
}

// SetBlocked transitions a task to "blocked" from any current status and
// records the reason in blocked_reason.
func SetBlocked(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID, reason string) (bool, error) {
	ok, err := GuardedTransition(ctx, pool, workspaceID, taskUUID, "*", "blocked", map[string]any{"blocked_reason": reason})
	if err != nil {
		return false, fmt.Errorf("SetBlocked: %w", err)
	}
	return ok, nil
}

// SetDone transitions a task from "in_review" to "done".
// Returns (false, nil) when the task was not in "in_review".
func SetDone(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID) (bool, error) {
	ok, err := GuardedTransition(ctx, pool, workspaceID, taskUUID, "in_review", "done", nil)
	if err != nil {
		return false, fmt.Errorf("SetDone: %w", err)
	}
	return ok, nil
}
