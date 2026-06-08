package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tiendv89/workflow-orchestrator/internal/database/queries"
)

// GoFeatureSpec describes a go-owned feature to be materialized into the DB.
type GoFeatureSpec struct {
	WorkspaceID    uuid.UUID    `json:"workspace_id"`
	OrganizationID uuid.UUID    `json:"organization_id"`
	Slug           string       `json:"slug"`
	Title          string       `json:"title"`
	Tasks          []GoTaskSpec `json:"tasks"`
}

// GoTaskSpec describes a single task within a go-owned feature.
type GoTaskSpec struct {
	Name      string   `json:"name"`
	Title     string   `json:"title"`
	Repo      string   `json:"repo"`
	DependsOn []string `json:"depends_on"`
	ActorType string   `json:"actor_type"`
}

const ownerGo = "go"

// CreateFeature inserts a go-owned feature into workspace_features and returns
// the auto-generated feature_id UUID.
func CreateFeature(ctx context.Context, pool *pgxpool.Pool, spec GoFeatureSpec) (uuid.UUID, error) {
	q := queries.New(pool)
	featureID := uuid.New()
	owner := ownerGo
	status := "in_design"
	stage := "product_spec"

	row, err := q.InsertFeature(ctx, queries.InsertFeatureParams{
		WorkspaceID:   spec.WorkspaceID,
		FeatureID:     featureID,
		FeatureName:   spec.Slug,
		Title:         spec.Title,
		FeatureStatus: &status,
		CurrentStage:  &stage,
		SourcePath:    nil,
		Owner:         &owner,
	})
	if err != nil {
		return uuid.Nil, fmt.Errorf("CreateFeature: insert failed: %w", err)
	}
	return row.FeatureID, nil
}

// CreateTask inserts a go-owned task into workspace_tasks and returns the
// auto-generated task_id UUID.
func CreateTask(ctx context.Context, pool *pgxpool.Pool, featureID uuid.UUID, featureSlug string, workspaceID uuid.UUID, t GoTaskSpec) (uuid.UUID, error) {
	q := queries.New(pool)
	taskID := uuid.New()
	owner := ownerGo
	status := "todo"

	dependsOnJSON, err := json.Marshal(t.DependsOn)
	if err != nil {
		return uuid.Nil, fmt.Errorf("CreateTask: marshal depends_on: %w", err)
	}

	var repo *string
	if t.Repo != "" {
		r := t.Repo
		repo = &r
	}

	row, err := q.InsertTask(ctx, queries.InsertTaskParams{
		WorkspaceID: workspaceID,
		FeatureID:   featureID,
		FeatureName: featureSlug,
		TaskID:      taskID,
		TaskName:    t.Name,
		Title:       t.Title,
		Repo:        repo,
		Status:      &status,
		DependsOn:   dependsOnJSON,
		Branch:      nil,
		SourcePath:  nil,
		Owner:       &owner,
	})
	if err != nil {
		return uuid.Nil, fmt.Errorf("CreateTask %s: insert failed: %w", t.Name, err)
	}
	return row.TaskID, nil
}

// InitialAutoReady advances tasks with an empty depends_on to status='ready'.
// This mirrors the auto-ready rule applied at feature-seed time.
func InitialAutoReady(ctx context.Context, pool *pgxpool.Pool, workspaceID, featureID uuid.UUID) error {
	_, err := pool.Exec(ctx, `
		UPDATE workspace_tasks
		SET    status     = 'ready',
		       updated_at = now()
		WHERE  workspace_id = $1
		  AND  feature_id   = $2
		  AND  depends_on   = '[]'::jsonb
		  AND  status       = 'todo'
	`, workspaceID, featureID)
	if err != nil {
		return fmt.Errorf("InitialAutoReady: %w", err)
	}
	return nil
}

// MaterializeFeature creates a feature and all its tasks in a single transaction,
// then seeds status='ready' for tasks that have no dependencies.
func MaterializeFeature(ctx context.Context, pool *pgxpool.Pool, spec GoFeatureSpec) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("MaterializeFeature: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	q := queries.New(tx)
	featureID := uuid.New()
	owner := ownerGo
	status := "in_design"
	stage := "product_spec"

	row, err := q.InsertFeature(ctx, queries.InsertFeatureParams{
		WorkspaceID:   spec.WorkspaceID,
		FeatureID:     featureID,
		FeatureName:   spec.Slug,
		Title:         spec.Title,
		FeatureStatus: &status,
		CurrentStage:  &stage,
		SourcePath:    nil,
		Owner:         &owner,
	})
	if err != nil {
		return fmt.Errorf("MaterializeFeature: insert feature: %w", err)
	}
	featureID = row.FeatureID

	for _, t := range spec.Tasks {
		taskID := uuid.New()
		taskOwner := ownerGo
		taskStatus := "todo"

		dependsOnJSON, err := json.Marshal(t.DependsOn)
		if err != nil {
			return fmt.Errorf("MaterializeFeature: marshal depends_on for %s: %w", t.Name, err)
		}

		var repo *string
		if t.Repo != "" {
			r := t.Repo
			repo = &r
		}

		if _, err := q.InsertTask(ctx, queries.InsertTaskParams{
			WorkspaceID: spec.WorkspaceID,
			FeatureID:   featureID,
			FeatureName: spec.Slug,
			TaskID:      taskID,
			TaskName:    t.Name,
			Title:       t.Title,
			Repo:        repo,
			Status:      &taskStatus,
			DependsOn:   dependsOnJSON,
			Branch:      nil,
			SourcePath:  nil,
			Owner:       &taskOwner,
		}); err != nil {
			return fmt.Errorf("MaterializeFeature: insert task %s: %w", t.Name, err)
		}
	}

	// Advance tasks with empty depends_on to 'ready' within the same transaction.
	if _, err := tx.Exec(ctx, `
		UPDATE workspace_tasks
		SET    status     = 'ready',
		       updated_at = now()
		WHERE  workspace_id = $1
		  AND  feature_id   = $2
		  AND  depends_on   = '[]'::jsonb
		  AND  status       = 'todo'
	`, spec.WorkspaceID, featureID); err != nil {
		return fmt.Errorf("MaterializeFeature: InitialAutoReady: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("MaterializeFeature: commit: %w", err)
	}
	return nil
}
