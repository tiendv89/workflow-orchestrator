package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/tiendv89/workflow-orchestrator/internal/database/queries"
)

// AutoReadyDependents advances go-owned tasks from "todo" to "ready" when all
// their named dependencies are now in "done" status within the same workspace.
//
// Must be called within an existing transaction — the caller owns commit/rollback.
// doneTaskName is the task_name slug that was just marked done; it is used to
// filter candidate tasks to only those whose depends_on list contains it.
//
// Returns the UUIDs (task_id column) of every task that was advanced.
func AutoReadyDependents(
	ctx context.Context,
	tx pgx.Tx,
	workspaceID uuid.UUID,
	doneTaskName string,
) ([]uuid.UUID, error) {
	// Fetch all go-owned todo tasks whose depends_on list references doneTaskName.
	// We filter in SQL so we only load candidates, not every todo task.
	rows, err := tx.Query(ctx, `
		SELECT task_id, feature_id, task_name, depends_on
		FROM workspace_tasks
		WHERE workspace_id = $1
		  AND owner        = 'go'
		  AND status       = 'todo'
		  AND depends_on @> $2::jsonb
	`, workspaceID, mustMarshalJSON([]string{doneTaskName}))
	if err != nil {
		return nil, fmt.Errorf("AutoReadyDependents: query candidates: %w", err)
	}
	defer rows.Close()

	type candidate struct {
		taskID    uuid.UUID
		featureID uuid.UUID
		taskName  string
		dependsOn []string
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		var depRaw []byte
		if err := rows.Scan(&c.taskID, &c.featureID, &c.taskName, &depRaw); err != nil {
			return nil, fmt.Errorf("AutoReadyDependents: scan row: %w", err)
		}
		if err := json.Unmarshal(depRaw, &c.dependsOn); err != nil {
			return nil, fmt.Errorf("AutoReadyDependents: parse depends_on for %s: %w", c.taskName, err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("AutoReadyDependents: rows: %w", err)
	}

	q := queries.New(tx)
	var advanced []uuid.UUID

	for _, c := range candidates {
		allDone, err := allDepsAreDone(ctx, tx, workspaceID, c.dependsOn)
		if err != nil {
			return nil, fmt.Errorf("AutoReadyDependents: check deps for %s: %w", c.taskName, err)
		}
		if !allDone {
			continue
		}

		// Guarded UPDATE: only advance if still todo (prevents double-advance).
		var advancedID uuid.UUID
		err = tx.QueryRow(ctx, `
			UPDATE workspace_tasks
			SET status = 'ready', updated_at = now()
			WHERE workspace_id = $1
			  AND task_id      = $2
			  AND status       = 'todo'
			RETURNING task_id
		`, workspaceID, c.taskID).Scan(&advancedID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// Another writer already advanced it — skip.
				continue
			}
			return nil, fmt.Errorf("AutoReadyDependents: update %s: %w", c.taskName, err)
		}

		note := "dependencies met"
		if err := appendLogInsert(ctx, q, workspaceID, c.featureID, c.taskID, "ready", "orchestrator", note); err != nil {
			return nil, fmt.Errorf("AutoReadyDependents: log %s: %w", c.taskName, err)
		}

		advanced = append(advanced, advancedID)
	}

	return advanced, nil
}

// allDepsAreDone returns true if every task name in deps is in "done" status
// within the given workspace.
func allDepsAreDone(ctx context.Context, tx pgx.Tx, workspaceID uuid.UUID, deps []string) (bool, error) {
	if len(deps) == 0 {
		return true, nil
	}
	for _, dep := range deps {
		var status string
		err := tx.QueryRow(ctx, `
			SELECT status
			FROM workspace_tasks
			WHERE workspace_id = $1
			  AND task_name    = $2
			LIMIT 1
		`, workspaceID, dep).Scan(&status)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// Dependency task doesn't exist — treat as not done.
				return false, nil
			}
			return false, fmt.Errorf("allDepsAreDone: check %q: %w", dep, err)
		}
		if status != "done" {
			return false, nil
		}
	}
	return true, nil
}

// mustMarshalJSON marshals v to JSON; panics on error (only safe for static values).
func mustMarshalJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("mustMarshalJSON: %v", err))
	}
	return b
}
