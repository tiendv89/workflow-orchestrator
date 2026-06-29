package queries

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

type Model struct {
	ID          uuid.UUID          `json:"id"`
	ModelID     string             `json:"model_id"`
	DisplayName *string            `json:"display_name"`
	Active      bool               `json:"active"`
	CreatedAt   pgtype.Timestamptz `json:"created_at"`
	UpdatedAt   pgtype.Timestamptz `json:"updated_at"`
}

const getWorkspaceDefaultModel = `-- name: GetWorkspaceDefaultModel :one
SELECT m.id, m.model_id, m.display_name, m.active, m.created_at, m.updated_at
FROM workspace_model_policies wmp
JOIN models m ON m.id = wmp.model_id
WHERE wmp.workspace_id = $1
  AND wmp.phase        = $2
  AND wmp.is_default   = true
LIMIT 1
`

type GetWorkspaceDefaultModelParams struct {
	WorkspaceID uuid.UUID `json:"workspace_id"`
	Phase       string    `json:"phase"`
}

func (q *Queries) GetWorkspaceDefaultModel(ctx context.Context, arg GetWorkspaceDefaultModelParams) (Model, error) {
	row := q.db.QueryRow(ctx, getWorkspaceDefaultModel, arg.WorkspaceID, arg.Phase)
	var i Model
	err := row.Scan(
		&i.ID,
		&i.ModelID,
		&i.DisplayName,
		&i.Active,
		&i.CreatedAt,
		&i.UpdatedAt,
	)
	return i, err
}
