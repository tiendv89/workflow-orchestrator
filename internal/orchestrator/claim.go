package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tiendv89/workflow-orchestrator/internal/database/queries"
)

// ClaimTask atomically claims a task by transitioning it from "ready" to
// "in_progress". It uses a guarded UPDATE so that at most one concurrent
// caller wins the claim (first-write-wins).
//
// Returns:
//   - (true, nil)  — claim won; the task is now in_progress.
//   - (false, nil) — claim lost (task was not in "ready" state); not an error.
//   - (false, err) — a database error occurred.
func ClaimTask(
	ctx context.Context,
	pool *pgxpool.Pool,
	workspaceID, taskUUID uuid.UUID,
	featureName, taskName, executorID string,
) (bool, error) {
	execution, err := buildExecution(executorID)
	if err != nil {
		return false, fmt.Errorf("claim: build execution payload: %w", err)
	}

	ready := "ready"
	inProgress := "in_progress"
	branch := TaskBranchName(featureName, taskName)

	q := queries.New(pool)
	_, err = q.GuardedUpdateTaskStatus(ctx, queries.GuardedUpdateTaskStatusParams{
		NewStatus:      &inProgress,
		Execution:      execution,
		Branch:         &branch,
		WorkspaceID:    workspaceID,
		TaskID:         taskUUID,
		ExpectedStatus: &ready,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("claim: guarded update: %w", err)
	}
	return true, nil
}

// RollbackClaim transitions a task from "in_progress" back to "ready" when
// dispatch fails after a successful claim. This prevents the task from becoming
// permanently orphaned — FindEligibleTasks only selects "ready" tasks, so
// without rollback the task would be stuck forever.
//
// Returns:
//   - (true, nil)  — rollback succeeded; the task is ready again.
//   - (false, nil) — the task was no longer in_progress (e.g. already claimed by another); not an error.
//   - (false, err) — a database error occurred.
func RollbackClaim(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID) (bool, error) {
	ok, err := GuardedTransition(ctx, pool, workspaceID, taskUUID, "in_progress", "ready", nil)
	if err != nil {
		return false, fmt.Errorf("RollbackClaim: %w", err)
	}
	return ok, nil
}

// buildExecution constructs the JSON execution payload for the claim.
func buildExecution(executorID string) ([]byte, error) {
	payload := map[string]string{
		"last_updated_by": executorID,
		"last_updated_at": time.Now().UTC().Format(time.RFC3339),
	}
	return json.Marshal(payload)
}
