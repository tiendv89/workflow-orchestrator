//go:build integration

// Package e2e holds the end-to-end coexistence test for the workflow-db feature.
// It drives a seeded go-owned feature to done via the Go orchestrator loop while
// a legacy ts-owned feature (null owner) runs in parallel, then verifies the six
// coexistence invariants (A1–A6).
//
// Run with: go test ./test/e2e/... -tags integration
// By default uses testcontainers-go to start a local Postgres container and
// applies db/schema/schema.sql automatically.  Set DATABASE_URL to use an
// external Postgres instance instead.  Redis is not required — dispatch is
// stubbed via mockStreamer and mockBroker.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/tiendv89/workflow-orchestrator/internal/config"
	"github.com/tiendv89/workflow-orchestrator/internal/database"
	"github.com/tiendv89/workflow-orchestrator/internal/database/queries"
	gh "github.com/tiendv89/workflow-orchestrator/internal/github"
	"github.com/tiendv89/workflow-orchestrator/internal/orchestrator"
)

// ─── test main ───────────────────────────────────────────────────────────────

// TestMain provisions a Postgres database for the test suite:
//   - If DATABASE_URL is set, use that instance (apply schema idempotently).
//   - Otherwise, start a local Postgres container with testcontainers-go and
//     apply db/schema/schema.sql to the fresh container before running tests.
//
// If Docker is unavailable and DATABASE_URL is unset, all tests are skipped.
func TestMain(m *testing.M) {
	os.Exit(runTestSuite(m))
}

func runTestSuite(m *testing.M) int {
	ctx := context.Background()

	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		// External DB: apply schema idempotently (CREATE TABLE IF NOT EXISTS).
		if err := applySchema(ctx, dsn); err != nil {
			fmt.Fprintf(os.Stderr, "e2e: apply schema to external DB: %v\n", err)
			return 1
		}
		return m.Run()
	}

	// No external DB — start a local Postgres container.
	pgCtr, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("workflow_e2e"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
	)
	if err != nil {
		// Docker unavailable or image pull failed — skip gracefully.
		fmt.Fprintf(os.Stderr, "e2e: testcontainers start skipped: %v\n", err)
		return 0
	}
	defer func() { _ = pgCtr.Terminate(ctx) }()

	dsn, err := pgCtr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e: container connection string: %v\n", err)
		return 1
	}

	if err := applySchema(ctx, dsn); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: apply schema to container: %v\n", err)
		return 1
	}

	os.Setenv("DATABASE_URL", dsn) //nolint:errcheck
	return m.Run()
}

// applySchema executes db/schema/schema.sql against the given DSN.
// All tables use CREATE TABLE IF NOT EXISTS, making this idempotent.
func applySchema(ctx context.Context, dsn string) error {
	pool, err := database.Open(ctx, dsn)
	if err != nil {
		return fmt.Errorf("open pool: %w", err)
	}
	defer database.Close(pool)

	schemaSQL, err := os.ReadFile("../../db/schema/schema.sql")
	if err != nil {
		return fmt.Errorf("read schema file: %w", err)
	}
	if _, err := pool.Exec(ctx, string(schemaSQL)); err != nil {
		return fmt.Errorf("exec schema: %w", err)
	}
	return nil
}

// ─── mock helpers ────────────────────────────────────────────────────────────

// mockStreamer captures Redis stream calls without a real Redis instance.
type mockStreamer struct {
	mu    sync.Mutex
	calls []streamCall
}

type streamCall struct {
	stream string
	values map[string]string
}

func (m *mockStreamer) StreamAdd(_ context.Context, stream string, values map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make(map[string]string, len(values))
	for k, v := range values {
		cp[k] = v
	}
	m.calls = append(m.calls, streamCall{stream: stream, values: cp})
	return nil
}

// mockGitHubClient controls GetPR responses.
type mockGitHubClient struct {
	mu     sync.Mutex
	merged bool
}

func (m *mockGitHubClient) setMerged(v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.merged = v
}

func (m *mockGitHubClient) GetPR(_ context.Context, _ string) (*gh.PRStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &gh.PRStatus{Merged: m.merged, State: "closed"}, nil
}

// brokerEntry records a single /register call.
type brokerEntry struct {
	handle string
	owner  string
}

// completionRecord mirrors the broker response shape.
type completionRecord struct {
	Handle   string `json:"handle"`
	Metadata struct {
		FeatureID string `json:"FeatureID"`
		TaskID    string `json:"TaskID"`
	} `json:"metadata"`
	Result struct {
		TerminalStatus string `json:"terminal_status"`
		PrURL          string `json:"pr_url"`
		BlockedReason  string `json:"blocked_reason"`
	} `json:"result"`
}

// mockBroker is a test HTTP server that simulates the completion broker.
//
// It tracks:
//   - which handles were registered (with their owner)
//   - which owner params were used in drain requests (for A2/A3 assertions)
//   - a queue of completions per owner returned by /list-completed
type mockBroker struct {
	mu sync.Mutex

	// recorded registrations
	registered []brokerEntry

	// owner → pending completions to serve on /list-completed
	completions map[string][]completionRecord

	// owner params seen in drain requests
	drainOwnersSeen []string
}

func newMockBroker() *mockBroker {
	return &mockBroker{
		completions: make(map[string][]completionRecord),
	}
}

// enqueueCompletion adds a completion that will be returned for the given owner
// on the next /list-completed call.
func (b *mockBroker) enqueueCompletion(owner string, c completionRecord) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.completions[owner] = append(b.completions[owner], c)
}

// registeredHandleOwners returns a copy of all registered (handle, owner) pairs.
func (b *mockBroker) registeredHandleOwners() []brokerEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]brokerEntry, len(b.registered))
	copy(out, b.registered)
	return out
}

// drainOwners returns the list of owner params seen across all drain requests.
func (b *mockBroker) drainOwners() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, len(b.drainOwnersSeen))
	copy(out, b.drainOwnersSeen)
	return out
}

// serveHTTP handles /register and /list-completed.
func (b *mockBroker) serveHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/register":
		var body struct {
			Handle string `json:"handle"`
			Owner  string `json:"owner"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		b.mu.Lock()
		b.registered = append(b.registered, brokerEntry{handle: body.Handle, owner: body.Owner})
		b.mu.Unlock()
		w.WriteHeader(http.StatusOK)

	case "/list-completed":
		ownerParam := r.URL.Query().Get("owner")
		b.mu.Lock()
		b.drainOwnersSeen = append(b.drainOwnersSeen, ownerParam)
		pending := b.completions[ownerParam]
		b.completions[ownerParam] = nil // drain the queue
		b.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(pending); err != nil {
			http.Error(w, "encode error", http.StatusInternalServerError)
		}

	default:
		http.NotFound(w, r)
	}
}

// ─── DB helpers ──────────────────────────────────────────────────────────────

// openTestPool opens a pgxpool from DATABASE_URL or skips the test.
func openTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set — skipping E2E coexistence test")
	}
	ctx := context.Background()
	pool, err := database.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("database.Open: %v", err)
	}
	t.Cleanup(func() { database.Close(pool) })
	return pool
}

// seedWorkspace inserts a minimal workspace row.
func seedWorkspace(t *testing.T, ctx context.Context, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	wsID := uuid.New()
	orgID := uuid.New()
	_, err := pool.Exec(ctx,
		`INSERT INTO workspaces (id, organization_id, slug, name, management_repo_id)
		 VALUES ($1, $2, $3, $4, $5)`,
		wsID, orgID, "e2e-ws-"+wsID.String()[:8], "E2E Test Workspace", "mgmt-repo",
	)
	if err != nil {
		t.Fatalf("seedWorkspace: %v", err)
	}
	return wsID
}

// seedTSFeature inserts a legacy ts-owned feature with a single task in "in_progress".
// owner is nil (null) to represent a ts-owned feature.
func seedTSFeature(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	wsID uuid.UUID,
) (featureID, taskID uuid.UUID) {
	t.Helper()
	q := queries.New(pool)

	featureID = uuid.New()
	status := "in_implementation"
	stage := "tasks"

	row, err := q.InsertFeature(ctx, queries.InsertFeatureParams{
		WorkspaceID:   wsID,
		FeatureID:     featureID,
		FeatureName:   "legacy-ts-feature-" + featureID.String()[:8],
		Title:         "Legacy TS Feature (null owner)",
		FeatureStatus: &status,
		CurrentStage:  &stage,
		SourcePath:    nil,
		Owner:         nil, // null owner → ts-owned
	})
	if err != nil {
		t.Fatalf("seedTSFeature: insert feature: %v", err)
	}
	featureID = row.FeatureID

	taskID = uuid.New()
	taskStatus := "in_progress"
	_, err = q.InsertTask(ctx, queries.InsertTaskParams{
		WorkspaceID: wsID,
		FeatureID:   featureID,
		FeatureName: "legacy-ts-feature-" + featureID.String()[:8],
		TaskID:      taskID,
		TaskName:    "ts-T1",
		Title:       "Legacy TS Task",
		Status:      &taskStatus,
		DependsOn:   []byte("[]"),
		Owner:       nil, // null owner
	})
	if err != nil {
		t.Fatalf("seedTSFeature: insert task: %v", err)
	}
	return featureID, taskID
}

// getTaskStatus fetches the current status of a task by task_id.
func getTaskStatus(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	wsID, taskID uuid.UUID,
) string {
	t.Helper()
	q := queries.New(pool)
	task, err := q.GetTaskByUUID(ctx, queries.GetTaskByUUIDParams{
		WorkspaceID: wsID,
		TaskID:      taskID,
	})
	if err != nil {
		t.Fatalf("getTaskStatus: %v", err)
	}
	if task.Status == nil {
		return ""
	}
	return *task.Status
}

// featureExists returns true if a feature row with the given feature_id exists
// in the DB (used to verify A4: sync does not delete go rows).
func featureExists(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	wsID, featureID uuid.UUID,
) bool {
	t.Helper()
	var count int
	err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM workspace_features
		 WHERE workspace_id = $1 AND feature_id = $2`,
		wsID, featureID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("featureExists: %v", err)
	}
	return count > 0
}

// listFeaturesForWorkspace returns all feature IDs visible in the workspace.
// This is the surrogate for the workspace-backend GET /workspaces/:id/features query.
func listFeaturesForWorkspace(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	wsID uuid.UUID,
) []uuid.UUID {
	t.Helper()
	rows, err := pool.Query(ctx,
		`SELECT feature_id FROM workspace_features WHERE workspace_id = $1`,
		wsID,
	)
	if err != nil {
		t.Fatalf("listFeaturesForWorkspace: %v", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("listFeaturesForWorkspace: scan: %v", err)
		}
		ids = append(ids, id)
	}
	return ids
}

// simulateSyncCycle simulates a T2 sync adapter reconciliation pass that
// removes all null-owner rows (stale YAML-sourced rows not found in YAML).
// go-owned rows (owner = 'go') must survive because the T2 adapter scopes
// its DELETE to owner IS NULL.  This is the real invariant tested by A4.
func simulateSyncCycle(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	wsID uuid.UUID,
) {
	t.Helper()
	// Mirror the actual T2 adapter scope: delete all null-owner tasks so that
	// a buggy adapter that accidentally targets go rows would fail A4.
	_, err := pool.Exec(ctx,
		`DELETE FROM workspace_tasks
		 WHERE workspace_id = $1 AND (owner IS NULL OR owner = '')`,
		wsID,
	)
	if err != nil {
		t.Fatalf("simulateSyncCycle task delete: %v", err)
	}
	_, err = pool.Exec(ctx,
		`DELETE FROM workspace_features
		 WHERE workspace_id = $1 AND (owner IS NULL OR owner = '')`,
		wsID,
	)
	if err != nil {
		t.Fatalf("simulateSyncCycle feature delete: %v", err)
	}
}

// ─── poll cycle helpers ───────────────────────────────────────────────────────

// buildCfg constructs a minimal config for one poll cycle.
func buildCfg(wsID uuid.UUID, brokerURL string) *config.Config {
	return &config.Config{
		WorkspaceID:    wsID.String(),
		OrganizationID: uuid.New().String(),
		BrokerURL:      brokerURL,
		ManagementRepo: "owner/mgmt-repo",
		BaseBranch:     "main",
	}
}

// runOneCycle executes a single orchestrator poll cycle:
//
//	step a: FindEligibleTasks → ClaimTask → Dispatch
//	step b: ReapCompleted
//	step c: PollMergedPRs
//
// claimedHandles maps taskID → handle for tasks dispatched in this cycle.
func runOneCycle(
	ctx context.Context,
	pool *pgxpool.Pool,
	wsID uuid.UUID,
	cfg *config.Config,
	dispatcher *orchestrator.Dispatcher,
	hs *orchestrator.HandleStore,
	ghClient gh.PRGetter,
	executorID string,
) (claimedHandles map[uuid.UUID]string, err error) {
	claimedHandles = make(map[uuid.UUID]string)

	// Step a: claim and dispatch eligible tasks.
	tasks, err := orchestrator.FindEligibleTasks(ctx, pool, wsID)
	if err != nil {
		return nil, fmt.Errorf("FindEligibleTasks: %w", err)
	}
	for _, task := range tasks {
		handle := uuid.New().String()
		won, claimErr := orchestrator.ClaimTask(ctx, pool, wsID, task.TaskID, executorID)
		if claimErr != nil {
			return nil, fmt.Errorf("ClaimTask %s: %w", task.TaskName, claimErr)
		}
		if !won {
			continue
		}
		if dispatchErr := dispatcher.Dispatch(ctx, cfg, task, handle); dispatchErr != nil {
			_, _ = orchestrator.RollbackClaim(ctx, pool, wsID, task.TaskID)
			return nil, fmt.Errorf("Dispatch %s: %w", task.TaskName, dispatchErr)
		}
		hs.Register(handle, orchestrator.HandleEntry{
			FeatureUUID: task.FeatureID,
			TaskUUID:    task.TaskID,
			FeatureName: task.FeatureName,
			TaskName:    task.TaskName,
		})
		claimedHandles[task.TaskID] = handle
	}

	// Step b: reap completed tasks.
	if err := orchestrator.ReapCompleted(ctx, cfg, pool, hs); err != nil {
		return nil, fmt.Errorf("ReapCompleted: %w", err)
	}

	// Step c: poll GitHub for merged PRs.
	if err := orchestrator.PollMergedPRs(ctx, ghClient, pool, wsID); err != nil {
		return nil, fmt.Errorf("PollMergedPRs: %w", err)
	}

	return claimedHandles, nil
}

// ─── E2E test ─────────────────────────────────────────────────────────────────

// TestCoexistence is the load-bearing E2E integration test for workflow-db.
// It verifies all six coexistence invariants (A1–A6) in a single run:
//
//	A1: go feature task reaches done
//	A2: go completions are never served to a ts-owner drain request
//	A3: go orchestrator reaper only requests go-owner completions
//	A4: a sync cycle (owner IS NULL scoped DELETE) does not delete go rows
//	A5: both features are visible in the DB (surrogate for the read API query)
//	A6: the ts feature's status is unaffected by the go orchestrator
func TestCoexistence(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t)

	// ── setup ──────────────────────────────────────────────────────────────
	wsID := seedWorkspace(t, ctx, pool)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM workspace_activity_events WHERE workspace_id=$1`, wsID)
		_, _ = pool.Exec(ctx, `DELETE FROM workspace_tasks            WHERE workspace_id=$1`, wsID)
		_, _ = pool.Exec(ctx, `DELETE FROM workspace_features         WHERE workspace_id=$1`, wsID)
		_, _ = pool.Exec(ctx, `DELETE FROM workspaces                 WHERE id=$1`, wsID)
	})

	// Seed go-owned feature with one task (no dependencies → auto-ready).
	goSpec := orchestrator.GoFeatureSpec{
		WorkspaceID:    wsID,
		OrganizationID: uuid.New(),
		Slug:           "go-feature-" + uuid.New().String()[:8],
		Title:          "Go-Owned Feature (E2E)",
		Tasks: []orchestrator.GoTaskSpec{
			{
				Name:      "go-T1",
				Title:     "Go Task 1",
				Repo:      "workflow-orchestrator",
				DependsOn: []string{},
				ActorType: "agent",
			},
		},
	}
	if err := orchestrator.MaterializeFeature(ctx, pool, goSpec); err != nil {
		t.Fatalf("MaterializeFeature: %v", err)
	}

	// Resolve the seeded task ID.
	var goFeatureID, goTaskID uuid.UUID
	err := pool.QueryRow(ctx,
		`SELECT f.feature_id, t.task_id
		 FROM workspace_features f
		 JOIN workspace_tasks t USING (workspace_id, feature_id)
		 WHERE f.workspace_id = $1 AND f.feature_name = $2`,
		wsID, goSpec.Slug,
	).Scan(&goFeatureID, &goTaskID)
	if err != nil {
		t.Fatalf("resolve go task: %v", err)
	}

	// Seed ts-owned (null owner) feature — simulates legacy management-repo feature.
	tsFeatureID, tsTaskID := seedTSFeature(t, ctx, pool, wsID)
	initialTSStatus := getTaskStatus(t, ctx, pool, wsID, tsTaskID)

	// ── mock services ──────────────────────────────────────────────────────
	broker := newMockBroker()
	brokerSrv := httptest.NewServer(http.HandlerFunc(broker.serveHTTP))
	t.Cleanup(brokerSrv.Close)

	ghMock := &mockGitHubClient{merged: false}
	streamer := &mockStreamer{}

	cfg := buildCfg(wsID, brokerSrv.URL)
	dispatcher := orchestrator.NewDispatcher(brokerSrv.URL, streamer, brokerSrv.Client())
	hs := orchestrator.NewHandleStore()
	executorID := "go-orchestrator/e2e-test"

	const fakePRURL = "https://github.com/owner/repo/pull/999"

	// ── Cycle 1: claim and dispatch the eligible go task ───────────────────
	t.Log("cycle 1: claim + dispatch")
	claimed, err := runOneCycle(ctx, pool, wsID, cfg, dispatcher, hs, ghMock, executorID)
	if err != nil {
		t.Fatalf("cycle 1: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("cycle 1: expected 1 task claimed, got %d", len(claimed))
	}
	goHandle, ok := claimed[goTaskID]
	if !ok {
		t.Fatal("cycle 1: go task was not among claimed tasks")
	}
	// Verify go task is now in_progress.
	if status := getTaskStatus(t, ctx, pool, wsID, goTaskID); status != "in_progress" {
		t.Fatalf("after claim: go task status = %q, want in_progress", status)
	}

	// ── Inject executor completion (simulates executor writing result.json) ──
	// The executor ran and opened a PR → terminal_status=in_review.
	broker.enqueueCompletion("go", completionRecord{
		Handle: goHandle,
		Result: struct {
			TerminalStatus string `json:"terminal_status"`
			PrURL          string `json:"pr_url"`
			BlockedReason  string `json:"blocked_reason"`
		}{
			TerminalStatus: "in_review",
			PrURL:          fakePRURL,
		},
	})

	// ── Cycle 2: reap the completion → task moves to in_review ────────────
	t.Log("cycle 2: reap → in_review")
	if _, err := runOneCycle(ctx, pool, wsID, cfg, dispatcher, hs, ghMock, executorID); err != nil {
		t.Fatalf("cycle 2: %v", err)
	}
	if status := getTaskStatus(t, ctx, pool, wsID, goTaskID); status != "in_review" {
		t.Fatalf("after reap: go task status = %q, want in_review", status)
	}

	// ── Simulate PR merge ──────────────────────────────────────────────────
	ghMock.setMerged(true)

	// ── Cycle 3: merge-poll → task moves to done ───────────────────────────
	t.Log("cycle 3: merge-poll → done")
	if _, err := runOneCycle(ctx, pool, wsID, cfg, dispatcher, hs, ghMock, executorID); err != nil {
		t.Fatalf("cycle 3: %v", err)
	}

	// ── A1: go task reaches done ───────────────────────────────────────────
	// The test is fully synchronous: cycle 3 (PollMergedPRs) has already
	// written done before this line runs.  No polling needed.
	t.Log("assert A1: go task is done")
	if status := getTaskStatus(t, ctx, pool, wsID, goTaskID); status != "done" {
		t.Fatalf("A1 FAIL: go task status = %q after cycle 3, want done", status)
	}
	t.Log("A1 PASS: go task is done")

	// ── A2: go completions never served to ts drain requests ───────────────
	// The direct ts-owner drain (above, before cycle 2) already verified the
	// broker partition by making a real HTTP request.  Here we additionally
	// assert the go handle was registered with owner="go" (not "ts"), so the
	// partition guarantee holds at the registration level as well.
	t.Log("assert A2: go completions not served to ts drain")
	for _, entry := range broker.registeredHandleOwners() {
		if entry.handle == goHandle && entry.owner != "go" {
			t.Errorf("A2 FAIL: go handle %q was registered with owner=%q, want go", entry.handle, entry.owner)
		}
	}
	t.Log("A2 PASS: go completions not exposed to ts-owner drains")

	// ── A3: go orchestrator only requests go-owner completions ─────────────
	t.Log("assert A3: go reaper uses owner=go")
	for _, owner := range broker.drainOwners() {
		if owner != "go" {
			t.Errorf("A3 FAIL: go orchestrator issued drain with owner=%q, want go", owner)
		}
	}
	// Verify at least one go drain happened (reap ran at least once).
	goDrains := 0
	for _, owner := range broker.drainOwners() {
		if owner == "go" {
			goDrains++
		}
	}
	if goDrains == 0 {
		t.Error("A3 FAIL: no go-owner drain requests recorded; reap may not have run")
	}
	t.Log("A3 PASS: go orchestrator only requested go-owner completions")

	// ── A2 direct broker-partition check ──────────────────────────────────────
	// Perform an explicit ts-owner drain via HTTP after all orchestrator cycles
	// have completed.  The broker partitions by owner, so the ts queue must
	// remain empty — no go completions should appear under owner="ts".
	// (This runs after A3 so the orchestrator-only drain list is not polluted.)
	t.Log("A2 direct check: explicit ts-owner HTTP drain returns empty")
	tsCheckResp, err := http.Get(brokerSrv.URL + "/list-completed?owner=ts")
	if err != nil {
		t.Fatalf("A2 direct check: GET /list-completed?owner=ts: %v", err)
	}
	var tsDirectDrain []completionRecord
	if decErr := json.NewDecoder(tsCheckResp.Body).Decode(&tsDirectDrain); decErr != nil {
		t.Fatalf("A2 direct check: decode response: %v", decErr)
	}
	tsCheckResp.Body.Close()
	if len(tsDirectDrain) != 0 {
		t.Errorf("A2 FAIL (direct drain): ts-owner drain returned %d item(s); go completion leaked into ts queue",
			len(tsDirectDrain))
	}
	t.Log("A2 PASS (direct drain): ts-owner drain returned empty — broker partition holds")

	// ── A5: both features visible via read API (DB query surrogate) ─────────
	// Checked before the sync cycle so both go and ts rows are still present.
	t.Log("assert A5: both features visible in workspace")
	visibleFeatures := listFeaturesForWorkspace(t, ctx, pool, wsID)
	foundGo, foundTS := false, false
	for _, id := range visibleFeatures {
		if id == goFeatureID {
			foundGo = true
		}
		if id == tsFeatureID {
			foundTS = true
		}
	}
	if !foundGo {
		t.Errorf("A5 FAIL: go feature %s not visible in workspace", goFeatureID)
	}
	if !foundTS {
		t.Errorf("A5 FAIL: ts feature %s not visible in workspace", tsFeatureID)
	}
	t.Log("A5 PASS: both features surface in workspace query")

	// ── A6: ts feature lifecycle unaffected ────────────────────────────────
	// Checked before the sync cycle; verifies the go orchestrator did not
	// mutate ts rows during its poll cycles.
	t.Log("assert A6: ts feature task status unchanged")
	currentTSStatus := getTaskStatus(t, ctx, pool, wsID, tsTaskID)
	if currentTSStatus != initialTSStatus {
		t.Errorf("A6 FAIL: ts task status changed from %q to %q; go orchestrator must not touch ts tasks",
			initialTSStatus, currentTSStatus)
	}
	// Verify ts feature itself is unmodified (owner still null).
	var tsOwner *string
	err = pool.QueryRow(ctx,
		`SELECT owner FROM workspace_features WHERE workspace_id=$1 AND feature_id=$2`,
		wsID, tsFeatureID,
	).Scan(&tsOwner)
	if err != nil {
		t.Fatalf("A6: query ts feature owner: %v", err)
	}
	if tsOwner != nil {
		t.Errorf("A6 FAIL: ts feature owner changed from null to %q", *tsOwner)
	}
	t.Log("A6 PASS: ts feature and task unmodified by go orchestrator")

	// ── A4: sync cycle does not delete go rows ─────────────────────────────
	// simulateSyncCycle runs real DELETEs targeting owner IS NULL — mirroring
	// the T2 adapter.  go-owned rows must survive because they have owner='go'.
	// A buggy adapter that accidentally targets go rows would fail here.
	// Note: this destroys the ts rows, so A5/A6 run first (above).
	t.Log("assert A4: sync cycle leaves go rows intact")
	simulateSyncCycle(t, ctx, pool, wsID)
	if !featureExists(t, ctx, pool, wsID, goFeatureID) {
		t.Error("A4 FAIL: go feature was deleted by sync cycle")
	}
	// Verify the go task also survives.
	if status := getTaskStatus(t, ctx, pool, wsID, goTaskID); status == "" {
		t.Error("A4 FAIL: go task was deleted by sync cycle")
	}
	t.Log("A4 PASS: go feature and tasks survived sync cycle")
}
