-- name: InsertActivityEvent :one
INSERT INTO workspace_activity_events (
    workspace_id,
    scope_type,
    feature_id,
    feature_name,
    task_id,
    task_name,
    action,
    actor,
    occurred_at,
    note,
    sequence,
    raw_event
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
)
RETURNING id;

-- name: GetNextActivitySequence :one
-- Returns the next sequence number for task-scoped activity events.
SELECT COALESCE(MAX(sequence), 0) + 1
FROM workspace_activity_events
WHERE workspace_id = $1
  AND feature_id   = $2
  AND task_id      = $3;
