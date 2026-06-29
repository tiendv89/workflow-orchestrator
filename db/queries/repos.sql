-- name: GetWorkspaceRepo :one
SELECT id, workspace_id, repo_id, base_branch, repo_url, created_at, updated_at
FROM workspace_repos
WHERE workspace_id = $1
  AND repo_id = $2
LIMIT 1;
