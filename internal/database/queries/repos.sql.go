package queries

import (
	"context"

	"github.com/google/uuid"
)

const getWorkspaceRepo = `-- name: GetWorkspaceRepo :one
SELECT id, workspace_id, repo_id, base_branch, repo_url, created_at, updated_at
FROM workspace_repos
WHERE workspace_id = $1
  AND repo_id = $2
LIMIT 1
`

type GetWorkspaceRepoParams struct {
	WorkspaceID uuid.UUID `json:"workspace_id"`
	RepoID      string    `json:"repo_id"`
}

func (q *Queries) GetWorkspaceRepo(ctx context.Context, arg GetWorkspaceRepoParams) (WorkspaceRepo, error) {
	row := q.db.QueryRow(ctx, getWorkspaceRepo, arg.WorkspaceID, arg.RepoID)
	var i WorkspaceRepo
	err := row.Scan(
		&i.ID,
		&i.WorkspaceID,
		&i.RepoID,
		&i.BaseBranch,
		&i.RepoURL,
		&i.CreatedAt,
		&i.UpdatedAt,
	)
	return i, err
}
