package orchestrator

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tiendv89/workflow-orchestrator/internal/database/queries"
)

// FindEligibleTasks returns all go-owned tasks in 'ready' status whose
// every dependency is already 'done' within the same feature.
//
// The query uses the (workspace_id, owner, status) index and performs the
// dependency check in SQL, so no secondary Go-side filter is needed when
// T10 (auto-ready) is wired. Until then the SQL dep filter acts as the
// defensive guard.
func FindEligibleTasks(ctx context.Context, pool *pgxpool.Pool, workspaceID uuid.UUID) ([]queries.WorkspaceTask, error) {
	q := queries.New(pool)
	tasks, err := q.ListEligibleTasks(ctx, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("FindEligibleTasks: %w", err)
	}
	return tasks, nil
}
