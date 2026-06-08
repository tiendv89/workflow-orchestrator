package orchestrator_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tiendv89/workflow-orchestrator/internal/database"
	"github.com/tiendv89/workflow-orchestrator/internal/database/queries"
	"github.com/tiendv89/workflow-orchestrator/internal/orchestrator"
)

// openPool returns a connected pool or skips the test if DATABASE_URL is unset.
func openPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set — skipping integration test")
	}
	pool, err := database.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("database.Open: %v", err)
	}
	t.Cleanup(func() { database.Close(pool) })
	return pool
}

// insertWorkspace inserts a minimal workspace and returns its UUID.
func insertWorkspace(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID uuid.UUID) uuid.UUID {
	t.Helper()
	wsID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO workspaces (id, organization_id, slug, name, management_repo_id)
		VALUES ($1, $2, $3, $4, $5)
	`, wsID, orgID, "ws-"+wsID.String(), "Test WS", "mgmt-repo")
	if err != nil {
		t.Fatalf("insert workspace: %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, `DELETE FROM workspace_tasks    WHERE workspace_id=$1`, wsID)
		pool.Exec(ctx, `DELETE FROM workspace_features WHERE workspace_id=$1`, wsID)
		pool.Exec(ctx, `DELETE FROM workspaces         WHERE id=$1`, wsID)
	})
	return wsID
}

func TestCreateFeature_Integration(t *testing.T) {
	pool := openPool(t)
	ctx := context.Background()
	orgID := uuid.New()
	wsID := insertWorkspace(t, ctx, pool, orgID)

	spec := orchestrator.GoFeatureSpec{
		WorkspaceID:    wsID,
		OrganizationID: orgID,
		Slug:           "test-feature",
		Title:          "Test Feature",
	}

	featureID, err := orchestrator.CreateFeature(ctx, pool, spec)
	if err != nil {
		t.Fatalf("CreateFeature: %v", err)
	}
	if featureID == uuid.Nil {
		t.Fatal("CreateFeature returned nil UUID")
	}

	q := queries.New(pool)
	row, err := q.GetFeatureByName(ctx, queries.GetFeatureByNameParams{
		WorkspaceID: wsID,
		FeatureName: "test-feature",
	})
	if err != nil {
		t.Fatalf("GetFeatureByName: %v", err)
	}
	if row.Owner == nil || *row.Owner != "go" {
		t.Errorf("owner = %v, want 'go'", row.Owner)
	}
	if row.SourcePath != nil {
		t.Errorf("source_path = %v, want NULL", *row.SourcePath)
	}
	if row.FeatureID != featureID {
		t.Errorf("feature_id mismatch: got %v, want %v", row.FeatureID, featureID)
	}
}

func TestCreateTask_Integration(t *testing.T) {
	pool := openPool(t)
	ctx := context.Background()
	orgID := uuid.New()
	wsID := insertWorkspace(t, ctx, pool, orgID)

	spec := orchestrator.GoFeatureSpec{
		WorkspaceID:    wsID,
		OrganizationID: orgID,
		Slug:           "feat-task-test",
		Title:          "Feature for task test",
	}
	featureID, err := orchestrator.CreateFeature(ctx, pool, spec)
	if err != nil {
		t.Fatalf("CreateFeature: %v", err)
	}

	taskSpec := orchestrator.GoTaskSpec{
		Name:      "T1",
		Title:     "First task",
		Repo:      "my-repo",
		DependsOn: []string{},
		ActorType: "agent",
	}
	taskID, err := orchestrator.CreateTask(ctx, pool, featureID, spec.Slug, wsID, taskSpec)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if taskID == uuid.Nil {
		t.Fatal("CreateTask returned nil UUID")
	}

	q := queries.New(pool)
	tasks, err := q.ListTasksByFeature(ctx, queries.ListTasksByFeatureParams{
		WorkspaceID: wsID,
		FeatureID:   featureID,
	})
	if err != nil {
		t.Fatalf("ListTasksByFeature: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("want 1 task, got %d", len(tasks))
	}
	tk := tasks[0]
	if tk.Owner == nil || *tk.Owner != "go" {
		t.Errorf("owner = %v, want 'go'", tk.Owner)
	}
	if tk.SourcePath != nil {
		t.Errorf("source_path = %v, want NULL", *tk.SourcePath)
	}
	if tk.Status == nil || *tk.Status != "todo" {
		t.Errorf("status = %v, want 'todo'", tk.Status)
	}
	var deps []string
	if err := json.Unmarshal(tk.DependsOn, &deps); err != nil {
		t.Fatalf("unmarshal depends_on: %v", err)
	}
	if len(deps) != 0 {
		t.Errorf("depends_on = %v, want []", deps)
	}
}

func TestCreateTask_WithDependsOn(t *testing.T) {
	pool := openPool(t)
	ctx := context.Background()
	orgID := uuid.New()
	wsID := insertWorkspace(t, ctx, pool, orgID)

	spec := orchestrator.GoFeatureSpec{
		WorkspaceID:    wsID,
		OrganizationID: orgID,
		Slug:           "feat-deps-test",
		Title:          "Feature deps test",
	}
	featureID, err := orchestrator.CreateFeature(ctx, pool, spec)
	if err != nil {
		t.Fatalf("CreateFeature: %v", err)
	}

	taskSpec := orchestrator.GoTaskSpec{
		Name:      "T2",
		Title:     "Second task",
		DependsOn: []string{"T1"},
	}
	taskID, err := orchestrator.CreateTask(ctx, pool, featureID, spec.Slug, wsID, taskSpec)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	q := queries.New(pool)
	tk, err := q.GetTaskByUUID(ctx, queries.GetTaskByUUIDParams{
		WorkspaceID: wsID,
		TaskID:      taskID,
	})
	if err != nil {
		t.Fatalf("GetTaskByUUID: %v", err)
	}
	var deps []string
	if err := json.Unmarshal(tk.DependsOn, &deps); err != nil {
		t.Fatalf("unmarshal depends_on: %v", err)
	}
	if len(deps) != 1 || deps[0] != "T1" {
		t.Errorf("depends_on = %v, want [T1]", deps)
	}
}

func TestInitialAutoReady_Integration(t *testing.T) {
	pool := openPool(t)
	ctx := context.Background()
	orgID := uuid.New()
	wsID := insertWorkspace(t, ctx, pool, orgID)

	spec := orchestrator.GoFeatureSpec{
		WorkspaceID:    wsID,
		OrganizationID: orgID,
		Slug:           "feat-autoready",
		Title:          "AutoReady Feature",
		Tasks: []orchestrator.GoTaskSpec{
			{Name: "T1", Title: "No deps", DependsOn: []string{}},
			{Name: "T2", Title: "Has dep", DependsOn: []string{"T1"}},
		},
	}

	featureID, err := orchestrator.CreateFeature(ctx, pool, spec)
	if err != nil {
		t.Fatalf("CreateFeature: %v", err)
	}
	for _, ts := range spec.Tasks {
		if _, err := orchestrator.CreateTask(ctx, pool, featureID, spec.Slug, wsID, ts); err != nil {
			t.Fatalf("CreateTask %s: %v", ts.Name, err)
		}
	}

	if err := orchestrator.InitialAutoReady(ctx, pool, wsID, featureID); err != nil {
		t.Fatalf("InitialAutoReady: %v", err)
	}

	q := queries.New(pool)
	tasks, err := q.ListTasksByFeature(ctx, queries.ListTasksByFeatureParams{
		WorkspaceID: wsID,
		FeatureID:   featureID,
	})
	if err != nil {
		t.Fatalf("ListTasksByFeature: %v", err)
	}

	statusByName := map[string]string{}
	for _, tk := range tasks {
		if tk.Status != nil {
			statusByName[tk.TaskName] = *tk.Status
		}
	}
	if statusByName["T1"] != "ready" {
		t.Errorf("T1 status = %q, want 'ready'", statusByName["T1"])
	}
	if statusByName["T2"] != "todo" {
		t.Errorf("T2 status = %q, want 'todo'", statusByName["T2"])
	}
}

func TestMaterializeFeature_Integration(t *testing.T) {
	pool := openPool(t)
	ctx := context.Background()
	orgID := uuid.New()
	wsID := insertWorkspace(t, ctx, pool, orgID)

	spec := orchestrator.GoFeatureSpec{
		WorkspaceID:    wsID,
		OrganizationID: orgID,
		Slug:           "feat-materialize",
		Title:          "Materialize Feature",
		Tasks: []orchestrator.GoTaskSpec{
			{Name: "T1", Title: "No deps", DependsOn: []string{}, ActorType: "agent"},
			{Name: "T2", Title: "Depends on T1", DependsOn: []string{"T1"}, ActorType: "agent"},
			{Name: "T3", Title: "Also no deps", DependsOn: []string{}, ActorType: "human"},
		},
	}

	if err := orchestrator.MaterializeFeature(ctx, pool, spec); err != nil {
		t.Fatalf("MaterializeFeature: %v", err)
	}

	q := queries.New(pool)
	feat, err := q.GetFeatureByName(ctx, queries.GetFeatureByNameParams{
		WorkspaceID: wsID,
		FeatureName: "feat-materialize",
	})
	if err != nil {
		t.Fatalf("GetFeatureByName: %v", err)
	}
	if feat.Owner == nil || *feat.Owner != "go" {
		t.Errorf("feature owner = %v, want 'go'", feat.Owner)
	}

	tasks, err := q.ListTasksByFeature(ctx, queries.ListTasksByFeatureParams{
		WorkspaceID: wsID,
		FeatureID:   feat.FeatureID,
	})
	if err != nil {
		t.Fatalf("ListTasksByFeature: %v", err)
	}
	if len(tasks) != 3 {
		t.Fatalf("want 3 tasks, got %d", len(tasks))
	}

	statusByName := map[string]string{}
	for _, tk := range tasks {
		if tk.Status != nil {
			statusByName[tk.TaskName] = *tk.Status
		}
	}
	if statusByName["T1"] != "ready" {
		t.Errorf("T1 status = %q, want 'ready'", statusByName["T1"])
	}
	if statusByName["T2"] != "todo" {
		t.Errorf("T2 status = %q, want 'todo'", statusByName["T2"])
	}
	if statusByName["T3"] != "ready" {
		t.Errorf("T3 status = %q, want 'ready'", statusByName["T3"])
	}
}
