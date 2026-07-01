package orchestrator

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CountInFlight returns the total number of in-flight dispatches across both
// workspace_tasks and handoff_prs for the given workspace.
//
// In-flight predicate (technical-design §"In-flight (soft-claim) predicate"):
//
//	workspace_tasks: owner='go' AND (status IN ('in_progress','reviewing') OR conflict_state='resolving')
//	handoff_prs:     conflict_state='resolving' (joined to handoffs for workspace scoping)
//
// Uses the partial indexes added by T1 for efficiency.
func CountInFlight(ctx context.Context, pool *pgxpool.Pool, workspaceID uuid.UUID) (int, error) {
	const sql = `
SELECT
  (
    SELECT COUNT(*)
    FROM workspace_tasks
    WHERE workspace_id = $1
      AND owner = 'go'
      AND (status IN ('in_progress', 'reviewing') OR conflict_state = 'resolving')
  )
  +
  (
    SELECT COUNT(*)
    FROM handoff_prs hp
    JOIN handoffs h ON h.id = hp.handoff_id
    WHERE h.workspace_id = $1
      AND hp.conflict_state = 'resolving'
  )
  AS total`

	var total int
	if err := pool.QueryRow(ctx, sql, workspaceID).Scan(&total); err != nil {
		return 0, fmt.Errorf("CountInFlight: %w", err)
	}
	return total, nil
}

// Headroom computes the available dispatch slots: max(0, maxInFlight - inflight).
// A non-positive result means no new dispatches should be attempted this cycle.
func Headroom(maxInFlight, inflight int) int {
	h := maxInFlight - inflight
	if h < 0 {
		return 0
	}
	return h
}
