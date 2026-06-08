package orchestrator_test

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tiendv89/workflow-orchestrator/internal/database"
	"github.com/tiendv89/workflow-orchestrator/internal/database/queries"
	"github.com/tiendv89/workflow-orchestrator/internal/orchestrator"
)

// testFixture holds IDs created for a single test run.
type testFixture struct {
	workspaceID uuid.UUID
	featureID   uuid.UUID // workspace_features.id (PK)
}

// setupFixture inserts a workspace and a feature row, returning their IDs.
// Each call creates isolated rows with unique slugs so tests don't interfere.
func setupFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) testFixture {
	t.Helper()

	wsID := uuid.New()
	orgID := uuid.New()
	slug := "test-" + wsID.String()[:8]

	_, err := pool.Exec(ctx,
		`INSERT INTO workspaces (id, organization_id, slug, name, management_repo_id)
		 VALUES ($1, $2, $3, $4, $5)`,
		wsID, orgID, slug, "Test Workspace", "test-repo-id",
	)
	if err != nil {
		t.Fatalf("insert workspace: %v", err)
	}

	q := queries.New(pool)
	featureID := uuid.New()
	owner := "go"
	featureStatus := "ready_for_implementation"
	currentStage := "tasks"
	row, err := q.InsertFeature(ctx, queries.InsertFeatureParams{
		WorkspaceID:   wsID,
		FeatureID:     featureID,
		FeatureName:   "test-feature-" + featureID.String()[:8],
		Title:         "Test Feature",
		FeatureStatus: &featureStatus,
		CurrentStage:  &currentStage,
		Owner:         &owner,
	})
	if err != nil {
		t.Fatalf("insert feature: %v", err)
	}

	t.Cleanup(func() {
		// Clean up in reverse FK order.
		pool.Exec(ctx, `DELETE FROM workspace_tasks WHERE workspace_id = $1`, wsID)
		pool.Exec(ctx, `DELETE FROM workspace_features WHERE workspace_id = $1`, wsID)
		pool.Exec(ctx, `DELETE FROM workspaces WHERE id = $1`, wsID)
	})

	return testFixture{workspaceID: wsID, featureID: row.ID}
}

// insertReadyTask inserts a task in "ready" status and returns its task_id.
func insertReadyTask(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fx testFixture) uuid.UUID {
	t.Helper()
	q := queries.New(pool)
	taskID := uuid.New()
	taskName := "T-" + taskID.String()[:8]
	status := "ready"
	repo := "test-repo"
	owner := "go"
	_, err := q.InsertTask(ctx, queries.InsertTaskParams{
		WorkspaceID: fx.workspaceID,
		FeatureID:   fx.featureID,
		FeatureName: "test-feature",
		TaskID:      taskID,
		TaskName:    taskName,
		Title:       "Test Task",
		Repo:        &repo,
		Status:      &status,
		DependsOn:   []byte("[]"),
		Owner:       &owner,
	})
	if err != nil {
		t.Fatalf("insertReadyTask: %v", err)
	}
	return taskID
}

func TestClaimTask_ConcurrencyExactlyOneWinner(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set — skipping integration test")
	}

	ctx := context.Background()
	pool, err := database.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("database.Open: %v", err)
	}
	defer database.Close(pool)

	fx := setupFixture(t, ctx, pool)
	taskID := insertReadyTask(t, ctx, pool, fx)

	const goroutines = 10
	results := make([]bool, goroutines)
	errs := make([]error, goroutines)

	var wg sync.WaitGroup
	ready := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-ready
			won, claimErr := orchestrator.ClaimTask(ctx, pool, fx.workspaceID, taskID, "executor-"+string(rune('A'+idx)))
			results[idx] = won
			errs[idx] = claimErr
		}(i)
	}
	close(ready)
	wg.Wait()

	for i, claimErr := range errs {
		if claimErr != nil {
			t.Errorf("goroutine %d returned error: %v", i, claimErr)
		}
	}
	winners := 0
	for _, won := range results {
		if won {
			winners++
		}
	}
	if winners != 1 {
		t.Errorf("expected exactly 1 winner, got %d", winners)
	}
}

func TestClaimTask_AlreadyInProgress(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set — skipping integration test")
	}

	ctx := context.Background()
	pool, err := database.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("database.Open: %v", err)
	}
	defer database.Close(pool)

	fx := setupFixture(t, ctx, pool)
	taskID := insertReadyTask(t, ctx, pool, fx)

	// First claim should win.
	won, err := orchestrator.ClaimTask(ctx, pool, fx.workspaceID, taskID, "executor-A")
	if err != nil {
		t.Fatalf("first ClaimTask returned error: %v", err)
	}
	if !won {
		t.Fatal("first ClaimTask should have won")
	}

	// Second claim on a task now in "in_progress" must return (false, nil).
	won, err = orchestrator.ClaimTask(ctx, pool, fx.workspaceID, taskID, "executor-B")
	if err != nil {
		t.Fatalf("second ClaimTask returned unexpected error: %v", err)
	}
	if won {
		t.Error("second ClaimTask should not win on a task already in_progress")
	}
}

func TestClaimTask_DBError(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set — skipping integration test")
	}

	ctx := context.Background()
	pool, err := database.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("database.Open: %v", err)
	}
	// Close the pool immediately to simulate a DB error.
	pool.Close()

	won, err := orchestrator.ClaimTask(ctx, pool, uuid.New(), uuid.New(), "executor-A")
	if err == nil {
		t.Fatal("expected a database error, got nil")
	}
	if won {
		t.Error("expected won=false on DB error")
	}
}
