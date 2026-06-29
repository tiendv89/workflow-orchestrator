-- name: GetWorkspaceDefaultModel :one
SELECT m.id, m.model_id, m.display_name, m.active, m.created_at, m.updated_at
FROM workspace_model_policies wmp
JOIN models m ON m.id = wmp.model_id
WHERE wmp.workspace_id = $1
  AND wmp.phase        = $2
  AND wmp.is_default   = true
LIMIT 1;
