-- name: GetTaskByUUID :one
SELECT
    id,
    workspace_id,
    feature_id,
    feature_name,
    task_id,
    task_name,
    title,
    repo,
    status,
    depends_on,
    blocked_reason,
    branch,
    execution,
    pr,
    workspace_pr,
    source_path,
    source_hash,
    owner,
    created_at,
    updated_at
FROM workspace_tasks
WHERE workspace_id = $1
  AND task_id = $2
LIMIT 1;

-- name: ListEligibleTasks :many
-- Returns go-owned tasks in 'ready' status whose every dependency task_name
-- is already in 'done' status within the same feature.
-- Used by T7 (eligibility scan).
SELECT t.*
FROM workspace_tasks t
WHERE t.workspace_id = $1
  AND t.owner = 'go'
  AND t.status = 'ready'
  AND NOT EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(t.depends_on) AS dep
      WHERE NOT EXISTS (
          SELECT 1
          FROM workspace_tasks dep_task
          WHERE dep_task.workspace_id = t.workspace_id
            AND dep_task.feature_id  = t.feature_id
            AND dep_task.task_name   = dep
            AND dep_task.status      = 'done'
      )
  )
ORDER BY t.created_at ASC;

-- name: InsertTask :one
INSERT INTO workspace_tasks (
    workspace_id,
    feature_id,
    feature_name,
    task_id,
    task_name,
    title,
    repo,
    status,
    depends_on,
    branch,
    source_path,
    owner,
    execution
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb
)
RETURNING id, task_id;

-- name: GuardedUpdateTaskStatus :one
-- Atomic FSM transition: updates status only if the current status matches
-- the expected value. Returns the row id if the update succeeded, or no rows
-- if another writer won the race (first-push-wins).
UPDATE workspace_tasks
SET
    status     = @new_status,
    execution  = COALESCE(@execution::jsonb, execution),
    branch     = COALESCE(@branch, branch),
    updated_at = now()
WHERE workspace_id = @workspace_id
  AND task_id      = @task_id
  AND status       = @expected_status
RETURNING id;

-- name: ListTasksByFeature :many
SELECT
    id,
    workspace_id,
    feature_id,
    feature_name,
    task_id,
    task_name,
    title,
    repo,
    status,
    depends_on,
    blocked_reason,
    branch,
    execution,
    pr,
    workspace_pr,
    source_path,
    source_hash,
    owner,
    created_at,
    updated_at
FROM workspace_tasks
WHERE workspace_id = $1
  AND feature_id   = $2
ORDER BY task_name;

-- name: ListInReviewTasksForOwner :many
-- Returns tasks in 'in_review' state for a given owner (e.g. 'go').
-- Used by the PR-merge poll (T13).
SELECT
    id,
    workspace_id,
    feature_id,
    feature_name,
    task_id,
    task_name,
    title,
    repo,
    status,
    depends_on,
    blocked_reason,
    branch,
    execution,
    pr,
    workspace_pr,
    source_path,
    source_hash,
    owner,
    created_at,
    updated_at
FROM workspace_tasks
WHERE workspace_id = $1
  AND owner        = $2
  AND status       = 'in_review';
