package orchestrator_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tiendv89/workflow-orchestrator/internal/database"
	"github.com/tiendv89/workflow-orchestrator/internal/database/queries"
	"github.com/tiendv89/workflow-orchestrator/internal/orchestrator"
)

// openTestDB connects to DATABASE_URL; skips the test if not set.
func openTestDB(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set — skipping integration test")
	}
	ctx := context.Background()
	pool, err := database.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("database.Open: %v", err)
	}
	t.Cleanup(func() { database.Close(pool) })
	return ctx, pool
}

// ptr returns a pointer to s (for nullable text columns).
func ptr(s string) *string { return &s }

// mustJSON encodes v as compact JSON bytes for jsonb columns.
func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("mustJSON: %v", err)
	}
	return b
}

// seedWorkspace inserts a minimal workspace row and returns its uuid.
func seedWorkspace(t *testing.T, ctx context.Context, pool *pgxpool.Pool, slug string) uuid.UUID {
	t.Helper()
	wsID := uuid.New()
	_, err := pool.Exec(ctx,
		`INSERT INTO workspaces (id, organization_id, slug, name, management_repo_id)
		 VALUES ($1, $2, $3, $4, $5)`,
		wsID, uuid.New(), slug+"-"+wsID.String(), "Test WS", "mgmt-repo",
	)
	if err != nil {
		t.Fatalf("seedWorkspace: %v", err)
	}
	return wsID
}

// seedTask inserts a single task row.
func seedTask(t *testing.T, ctx context.Context, q *queries.Queries,
	wsID, featureRowID uuid.UUID, featureName, taskName, status string,
	owner *string, deps []string,
) {
	t.Helper()
	if _, err := q.InsertTask(ctx, queries.InsertTaskParams{
		WorkspaceID: wsID,
		FeatureID:   featureRowID,
		FeatureName: featureName,
		TaskID:      uuid.New(),
		TaskName:    taskName,
		Title:       "Task " + taskName,
		Status:      ptr(status),
		DependsOn:   mustJSON(t, deps),
		Owner:       owner,
	}); err != nil {
		t.Fatalf("seedTask %q: %v", taskName, err)
	}
}

// TestFindEligibleTasks_ReturnsOnlyGoReadyTasks seeds tasks in a variety of
// states and owners and asserts that FindEligibleTasks returns exactly the
// go-owned, ready, dependency-satisfied subset.
func TestFindEligibleTasks_ReturnsOnlyGoReadyTasks(t *testing.T) {
	ctx, pool := openTestDB(t)
	q := queries.New(pool)

	wsID := seedWorkspace(t, ctx, pool, "elig")
	featureName := "feat-" + wsID.String()

	// Use raw pool.Exec for feature insert to control the feature_name.
	var featureRowID uuid.UUID
	row, err := q.InsertFeature(ctx, queries.InsertFeatureParams{
		WorkspaceID:   wsID,
		FeatureID:     uuid.New(),
		FeatureName:   featureName,
		Title:         "Eligibility Test Feature",
		FeatureStatus: ptr("in_implementation"),
		CurrentStage:  ptr("tasks"),
		Owner:         ptr("go"),
	})
	if err != nil {
		t.Fatalf("InsertFeature: %v", err)
	}
	featureRowID = row.FeatureID

	goOwner := ptr("go")
	noOwner := (*string)(nil)

	// T-done is used as a satisfied dependency for T-go-ready-deps-met.
	const doneName = "T-done"

	type taskSpec struct {
		name   string
		status string
		owner  *string
		deps   []string
	}

	taskSpecs := []taskSpec{
		// Eligible: go-owned, ready, no deps.
		{name: "T-go-ready-nodeps", status: "ready", owner: goOwner, deps: []string{}},
		// Eligible: go-owned, ready, dep satisfied (doneName is 'done').
		{name: "T-go-ready-deps-met", status: "ready", owner: goOwner, deps: []string{doneName}},
		// Not eligible: go-owned, ready, dep NOT satisfied (task doesn't exist).
		{name: "T-go-ready-deps-unmet", status: "ready", owner: goOwner, deps: []string{"T-nonexistent"}},
		// Not eligible: wrong status.
		{name: "T-go-todo", status: "todo", owner: goOwner, deps: []string{}},
		{name: "T-go-inprogress", status: "in_progress", owner: goOwner, deps: []string{}},
		{name: "T-go-blocked", status: "blocked", owner: goOwner, deps: []string{}},
		// Not eligible: ts-owned (owner IS NULL).
		{name: "T-ts-ready", status: "ready", owner: noOwner, deps: []string{}},
		// Seed the 'done' task that satisfies T-go-ready-deps-met's dependency.
		{name: doneName, status: "done", owner: goOwner, deps: []string{}},
	}

	for _, s := range taskSpecs {
		seedTask(t, ctx, q, wsID, featureRowID, featureName, s.name, s.status, s.owner, s.deps)
	}

	got, err := orchestrator.FindEligibleTasks(ctx, pool, wsID)
	if err != nil {
		t.Fatalf("FindEligibleTasks: %v", err)
	}

	gotNames := make(map[string]bool, len(got))
	for _, task := range got {
		gotNames[task.TaskName] = true
	}

	// Must be included.
	for _, want := range []string{"T-go-ready-nodeps", "T-go-ready-deps-met"} {
		if !gotNames[want] {
			t.Errorf("expected %q in eligible results, got names: %v", want, gotNames)
		}
	}

	// Must be excluded.
	for _, notWant := range []string{
		doneName,
		"T-go-ready-deps-unmet",
		"T-go-todo",
		"T-go-inprogress",
		"T-go-blocked",
		"T-ts-ready",
	} {
		if gotNames[notWant] {
			t.Errorf("did not expect %q in eligible results, but it was returned", notWant)
		}
	}

	// Invariant: every returned task must have owner='go' and status='ready'.
	for _, task := range got {
		if task.Owner == nil || *task.Owner != "go" {
			t.Errorf("task %q: owner = %v, want 'go'", task.TaskName, task.Owner)
		}
		if task.Status == nil || *task.Status != "ready" {
			t.Errorf("task %q: status = %v, want 'ready'", task.TaskName, task.Status)
		}
	}
}

// TestFindEligibleTasks_EmptyWorkspace verifies no error and empty slice when
// a workspace has no tasks.
func TestFindEligibleTasks_EmptyWorkspace(t *testing.T) {
	ctx, pool := openTestDB(t)

	wsID := seedWorkspace(t, ctx, pool, "empty")

	got, err := orchestrator.FindEligibleTasks(ctx, pool, wsID)
	if err != nil {
		t.Fatalf("FindEligibleTasks: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 tasks for empty workspace, got %d", len(got))
	}
}

// TestFindEligibleTasks_UnknownWorkspace verifies that a non-existent workspace
// UUID returns an empty slice without error.
func TestFindEligibleTasks_UnknownWorkspace(t *testing.T) {
	ctx, pool := openTestDB(t)

	got, err := orchestrator.FindEligibleTasks(ctx, pool, uuid.New())
	if err != nil {
		t.Fatalf("FindEligibleTasks with unknown workspace: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 tasks for unknown workspace, got %d", len(got))
	}
}

// TestFindEligibleTasks_QueryPlanUsesIndex runs EXPLAIN (FORMAT JSON, ANALYZE
// FALSE) on the listEligibleTasks query and asserts that the planner selects
// workspace_tasks_workspace_owner_status — the (workspace_id, owner, status) index
// created by migration 00015_owner.
func TestFindEligibleTasks_QueryPlanUsesIndex(t *testing.T) {
	ctx, pool := openTestDB(t)

	// Ensure the index exists. It is created by migration 00015_owner, but we
	// guard with IF NOT EXISTS for test-DB self-sufficiency so this test can
	// run in isolation.
	_, err := pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS workspace_tasks_workspace_owner_status
		ON workspace_tasks (workspace_id, owner, status)`)
	if err != nil {
		t.Fatalf("ensure index: %v", err)
	}

	// Acquire a dedicated connection so that SET enable_seqscan and EXPLAIN
	// share the same session — pgxpool may route calls to different conns.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire conn: %v", err)
	}
	defer conn.Release()

	// Disable sequential scans so the planner is forced to consider the index
	// even on a table with zero rows (where seq-scan cost would otherwise win).
	if _, err = conn.Exec(ctx, "SET enable_seqscan = off"); err != nil {
		t.Fatalf("SET enable_seqscan: %v", err)
	}

	// The SQL below is the exact text executed by queries.ListEligibleTasks.
	// Keep it in sync with db/queries/tasks.sql → listEligibleTasks.
	const explainSQL = `EXPLAIN (FORMAT JSON, ANALYZE FALSE)
SELECT t.id, t.workspace_id, t.feature_id, t.feature_name, t.task_id, t.task_name,
       t.title, t.repo, t.status, t.depends_on, t.blocked_reason, t.branch,
       t.execution, t.pr, t.workspace_pr, t.source_path, t.source_hash,
       t.owner, t.created_at, t.updated_at
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
  )`

	var planJSON string
	if err = conn.QueryRow(ctx, explainSQL, uuid.New()).Scan(&planJSON); err != nil {
		t.Fatalf("EXPLAIN scan: %v", err)
	}

	if !strings.Contains(planJSON, "workspace_tasks_workspace_owner_status") {
		t.Errorf("query plan does not reference workspace_tasks_workspace_owner_status;\nplan:\n%s", planJSON)
	}
}
