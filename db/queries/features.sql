-- name: GetFeatureByName :one
SELECT
    id,
    workspace_id,
    feature_id,
    feature_name,
    title,
    feature_status,
    current_stage,
    next_action,
    stages,
    source_path,
    source_hash,
    owner,
    created_at,
    updated_at
FROM workspace_features
WHERE workspace_id = $1
  AND feature_name = $2
LIMIT 1;

-- name: InsertFeature :one
INSERT INTO workspace_features (
    workspace_id,
    feature_id,
    feature_name,
    title,
    feature_status,
    current_stage,
    source_path,
    owner
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8
)
RETURNING id, feature_id;
