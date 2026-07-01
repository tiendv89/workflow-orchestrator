-- name: InsertHandoff :one
-- Creates a new handoff row. ON CONFLICT DO NOTHING makes this idempotent —
-- the UNIQUE(feature_id) constraint is the multi-instance trigger guard.
INSERT INTO handoffs (workspace_id, feature_id, status)
VALUES ($1, $2, 'open')
ON CONFLICT (feature_id) DO NOTHING
RETURNING id, workspace_id, feature_id, mgmt_pr_url, status, created_at, finalized_at;

-- name: GetHandoffByFeature :one
SELECT id, workspace_id, feature_id, mgmt_pr_url, status, created_at, finalized_at
FROM handoffs
WHERE feature_id = $1
LIMIT 1;

-- name: UpdateHandoffMgmtPRURL :exec
UPDATE handoffs
SET mgmt_pr_url = $2
WHERE id = $1;

-- name: FinalizeHandoff :exec
UPDATE handoffs
SET status = 'finalized', finalized_at = now()
WHERE id = $1;
