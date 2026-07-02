package orchestrator_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tiendv89/workflow-orchestrator/internal/config"
	"github.com/tiendv89/workflow-orchestrator/internal/orchestrator"
)

// --- feature status helpers ---

func setFeatureStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fx testFixture, status string) {
	t.Helper()
	_, err := pool.Exec(ctx,
		`UPDATE workspace_features SET feature_status=$1 WHERE feature_id=$2`,
		status, fx.featureID,
	)
	if err != nil {
		t.Fatalf("setFeatureStatus: %v", err)
	}
}

func getFeatureStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fx testFixture) string {
	t.Helper()
	var status *string
	if err := pool.QueryRow(ctx,
		`SELECT feature_status FROM workspace_features WHERE feature_id=$1`,
		fx.featureID,
	).Scan(&status); err != nil {
		t.Fatalf("getFeatureStatus: %v", err)
	}
	if status == nil {
		return ""
	}
	return *status
}

// cleanupHandoffs registers a t.Cleanup to delete handoffs for this fixture.
// Call immediately after setupFixture in any test that may create handoffs.
// The cleanup runs before setupFixture's cleanup (LIFO order), so FK
// constraints on workspace_features are satisfied.
func cleanupHandoffs(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fx testFixture) {
	t.Helper()
	t.Cleanup(func() {
		// handoff_prs is ON DELETE CASCADE from handoffs, so deleting handoffs
		// is sufficient.
		_, _ = pool.Exec(ctx, `DELETE FROM handoffs WHERE feature_id=$1`, fx.featureID)
	})
}

// minimalCfg returns a config that skips all GitHub calls:
//   - empty ManagementRepo → createMgmtHandoffPR is a no-op
//   - tasks with no matching workspace_repos row hit ErrNoRows in createHandoffPR
//     and are recorded as skipped_no_branch rather than an error.
func minimalCfg(workspaceID uuid.UUID) *config.Config {
	return &config.Config{
		WorkspaceID:    workspaceID.String(),
		OrganizationID: uuid.New().String(),
		BaseBranch:     "main",
		ManagementRepo: "", // skips mgmt PR creation
		BrokerURL:      "http://localhost:9999",
	}
}

// --- RunFeatureLifecycle tests ---

// TestRunFeatureLifecycle_ReadyToInImplementation verifies that a feature in
// ready_for_implementation transitions to in_implementation when a task is
// dispatched (status not in todo/ready/done/cancelled).
func TestRunFeatureLifecycle_ReadyToInImplementation(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	setFeatureStatus(t, ctx, pool, fx, "ready_for_implementation")
	insertTaskWithStatus(t, ctx, pool, fx, "in_progress") // dispatched

	if err := orchestrator.RunFeatureLifecycle(ctx, pool, minimalCfg(fx.workspaceID), nil, fx.workspaceID); err != nil {
		t.Fatalf("RunFeatureLifecycle: %v", err)
	}

	if got := getFeatureStatus(t, ctx, pool, fx); got != "in_implementation" {
		t.Errorf("feature_status = %q, want in_implementation", got)
	}
}

// TestRunFeatureLifecycle_NoDispatch_StaysReady verifies that when no tasks
// are dispatched, ready_for_implementation is unchanged.
func TestRunFeatureLifecycle_NoDispatch_StaysReady(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	setFeatureStatus(t, ctx, pool, fx, "ready_for_implementation")
	insertTaskWithStatus(t, ctx, pool, fx, "ready") // not dispatched

	if err := orchestrator.RunFeatureLifecycle(ctx, pool, minimalCfg(fx.workspaceID), nil, fx.workspaceID); err != nil {
		t.Fatalf("RunFeatureLifecycle: %v", err)
	}

	if got := getFeatureStatus(t, ctx, pool, fx); got != "ready_for_implementation" {
		t.Errorf("feature_status = %q, want ready_for_implementation (unchanged)", got)
	}
}

// TestRunFeatureLifecycle_AllDone_TriggersHandoff verifies the all-done →
// handoff path: feature → in_handoff, one handoff row created.
// Uses empty ManagementRepo and tasks with no matching workspace_repos row so
// no real GitHub calls are made (skipped_no_branch path).
func TestRunFeatureLifecycle_AllDone_TriggersHandoff(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	cleanupHandoffs(t, ctx, pool, fx)

	setFeatureStatus(t, ctx, pool, fx, "in_implementation")
	insertTaskWithStatus(t, ctx, pool, fx, "done")
	insertTaskWithStatus(t, ctx, pool, fx, "cancelled")

	if err := orchestrator.RunFeatureLifecycle(ctx, pool, minimalCfg(fx.workspaceID), nil, fx.workspaceID); err != nil {
		t.Fatalf("RunFeatureLifecycle: %v", err)
	}

	if got := getFeatureStatus(t, ctx, pool, fx); got != "in_handoff" {
		t.Errorf("feature_status = %q, want in_handoff", got)
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM handoffs WHERE feature_id=$1`, fx.featureID,
	).Scan(&count); err != nil {
		t.Fatalf("count handoffs: %v", err)
	}
	if count != 1 {
		t.Errorf("handoffs count = %d, want 1", count)
	}
}

// TestRunFeatureLifecycle_NotAllDone_NoHandoff verifies that a feature with at
// least one non-terminal task does NOT trigger handoff.
func TestRunFeatureLifecycle_NotAllDone_NoHandoff(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	setFeatureStatus(t, ctx, pool, fx, "in_implementation")
	insertTaskWithStatus(t, ctx, pool, fx, "done")
	insertTaskWithStatus(t, ctx, pool, fx, "in_review") // not terminal

	if err := orchestrator.RunFeatureLifecycle(ctx, pool, minimalCfg(fx.workspaceID), nil, fx.workspaceID); err != nil {
		t.Fatalf("RunFeatureLifecycle: %v", err)
	}

	if got := getFeatureStatus(t, ctx, pool, fx); got != "in_implementation" {
		t.Errorf("feature_status = %q, want in_implementation (unchanged)", got)
	}
}

// TestTriggerHandoff_Idempotent verifies that calling TriggerHandoff twice on
// the same feature creates exactly one handoff row (UNIQUE guard).
func TestTriggerHandoff_Idempotent(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	cleanupHandoffs(t, ctx, pool, fx)

	featureName := "test-feature-" + fx.featureID.String()[:8]
	cfg := minimalCfg(fx.workspaceID)

	// First trigger — moves feature to in_handoff and creates the row.
	setFeatureStatus(t, ctx, pool, fx, "in_implementation")
	if err := orchestrator.TriggerHandoff(ctx, pool, cfg, nil, fx.workspaceID, fx.featureID, featureName); err != nil {
		t.Fatalf("TriggerHandoff (1st): %v", err)
	}

	// Reset status so the function re-evaluates the guard.
	setFeatureStatus(t, ctx, pool, fx, "in_implementation")

	// Second trigger — UNIQUE(feature_id) fires; must be a no-op.
	if err := orchestrator.TriggerHandoff(ctx, pool, cfg, nil, fx.workspaceID, fx.featureID, featureName); err != nil {
		t.Fatalf("TriggerHandoff (2nd): %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM handoffs WHERE feature_id=$1`, fx.featureID,
	).Scan(&count); err != nil {
		t.Fatalf("count handoffs: %v", err)
	}
	if count != 1 {
		t.Errorf("handoffs count = %d, want 1 (idempotent)", count)
	}
}

// --- handoff PR conflict FSM helpers ---

// insertHandoffAndPR inserts a handoff row and one handoff_pr row, returning
// (handoffID, handoffPRID). Cleanup is registered via t.Cleanup.
func insertHandoffAndPR(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	fx testFixture,
	prStatus, conflictState string,
) (uuid.UUID, uuid.UUID) {
	t.Helper()

	handoffID := uuid.New()
	if _, err := pool.Exec(ctx,
		`INSERT INTO handoffs (id, workspace_id, feature_id, status)
		 VALUES ($1, $2, $3, 'open')`,
		handoffID, fx.workspaceID, fx.featureID,
	); err != nil {
		t.Fatalf("insertHandoffAndPR: insert handoff: %v", err)
	}

	prID := uuid.New()
	prURL := "https://github.com/test/repo/pull/1"
	if _, err := pool.Exec(ctx,
		`INSERT INTO handoff_prs (id, handoff_id, repo, pr_url, status, conflict_state)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		prID, handoffID, "test-repo", prURL, prStatus, conflictState,
	); err != nil {
		t.Fatalf("insertHandoffAndPR: insert handoff_pr: %v", err)
	}

	// handoff_prs cascades on handoff delete; only the handoff row needs deleting.
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM handoffs WHERE id=$1`, handoffID)
	})

	return handoffID, prID
}

func getHandoffPRConflictState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, prID uuid.UUID) string {
	t.Helper()
	var state string
	if err := pool.QueryRow(ctx,
		`SELECT conflict_state FROM handoff_prs WHERE id=$1`, prID,
	).Scan(&state); err != nil {
		t.Fatalf("getHandoffPRConflictState: %v", err)
	}
	return state
}

func getHandoffPRRebaseAttempts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, prID uuid.UUID) int32 {
	t.Helper()
	var attempts int32
	if err := pool.QueryRow(ctx,
		`SELECT rebase_attempts FROM handoff_prs WHERE id=$1`, prID,
	).Scan(&attempts); err != nil {
		t.Fatalf("getHandoffPRRebaseAttempts: %v", err)
	}
	return attempts
}

// --- handoff PR conflict FSM tests ---

// TestSetHandoffPRConflicted_FromNone verifies that SetHandoffPRConflicted
// marks a handoff PR as conflicted when in 'none' state.
func TestSetHandoffPRConflicted_FromNone(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	_, prID := insertHandoffAndPR(t, ctx, pool, fx, "open", "none")

	ok, err := orchestrator.SetHandoffPRConflicted(ctx, pool, prID)
	if err != nil {
		t.Fatalf("SetHandoffPRConflicted: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	if got := getHandoffPRConflictState(t, ctx, pool, prID); got != "conflicted" {
		t.Errorf("conflict_state = %q, want conflicted", got)
	}
}

// TestSetHandoffPRConflicted_GuardsResolving verifies that SetHandoffPRConflicted
// is a no-op while a rebase is in-flight (conflict_state='resolving').
func TestSetHandoffPRConflicted_GuardsResolving(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	_, prID := insertHandoffAndPR(t, ctx, pool, fx, "open", "resolving")

	ok, err := orchestrator.SetHandoffPRConflicted(ctx, pool, prID)
	if err != nil {
		t.Fatalf("SetHandoffPRConflicted: %v", err)
	}
	if ok {
		t.Error("expected ok=false when conflict_state=resolving (must not interrupt)")
	}
	if got := getHandoffPRConflictState(t, ctx, pool, prID); got != "resolving" {
		t.Errorf("conflict_state = %q, want resolving (unchanged)", got)
	}
}

// TestSetHandoffPRResolving_Claim verifies the claim transition.
func TestSetHandoffPRResolving_Claim(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	_, prID := insertHandoffAndPR(t, ctx, pool, fx, "open", "conflicted")

	handle := uuid.New().String()
	nonce := uuid.New().String()
	ok, err := orchestrator.SetHandoffPRResolving(ctx, pool, prID, handle, nonce)
	if err != nil {
		t.Fatalf("SetHandoffPRResolving: %v", err)
	}
	if !ok {
		t.Error("expected ok=true on first claim")
	}
	if got := getHandoffPRConflictState(t, ctx, pool, prID); got != "resolving" {
		t.Errorf("conflict_state = %q, want resolving", got)
	}
}

// TestSetHandoffPRResolving_SecondClaimFails verifies that only the first claim wins.
func TestSetHandoffPRResolving_SecondClaimFails(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	_, prID := insertHandoffAndPR(t, ctx, pool, fx, "open", "resolving")

	ok, err := orchestrator.SetHandoffPRResolving(ctx, pool, prID, uuid.New().String(), uuid.New().String())
	if err != nil {
		t.Fatalf("SetHandoffPRResolving: %v", err)
	}
	if ok {
		t.Error("expected ok=false when already resolving")
	}
}

// TestSetHandoffPRResolved verifies that SetHandoffPRResolved transitions
// resolving → resolved and resets rebase_attempts.
func TestSetHandoffPRResolved(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	_, prID := insertHandoffAndPR(t, ctx, pool, fx, "open", "resolving")
	if _, err := pool.Exec(ctx, `UPDATE handoff_prs SET rebase_attempts=2 WHERE id=$1`, prID); err != nil {
		t.Fatalf("preset rebase_attempts: %v", err)
	}

	ok, err := orchestrator.SetHandoffPRResolved(ctx, pool, prID)
	if err != nil {
		t.Fatalf("SetHandoffPRResolved: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	if got := getHandoffPRConflictState(t, ctx, pool, prID); got != "resolved" {
		t.Errorf("conflict_state = %q, want resolved", got)
	}
	if got := getHandoffPRRebaseAttempts(t, ctx, pool, prID); got != 0 {
		t.Errorf("rebase_attempts = %d, want 0 (reset on success)", got)
	}
}

// TestMarkHandoffPRRebaseRetry verifies retry: resolving → conflicted, counter++.
func TestMarkHandoffPRRebaseRetry(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	_, prID := insertHandoffAndPR(t, ctx, pool, fx, "open", "resolving")

	ok, err := orchestrator.MarkHandoffPRRebaseRetry(ctx, pool, prID)
	if err != nil {
		t.Fatalf("MarkHandoffPRRebaseRetry: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	if got := getHandoffPRConflictState(t, ctx, pool, prID); got != "conflicted" {
		t.Errorf("conflict_state = %q, want conflicted", got)
	}
	if got := getHandoffPRRebaseAttempts(t, ctx, pool, prID); got != 1 {
		t.Errorf("rebase_attempts = %d, want 1 (incremented)", got)
	}
}

// --- HandleHandoffPRRebaseCompletion tests ---

// TestHandleHandoffPRRebaseCompletion_Success verifies that a successful rebase
// marks the handoff PR resolved and resets rebase_attempts.
func TestHandleHandoffPRRebaseCompletion_Success(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	_, prID := insertHandoffAndPR(t, ctx, pool, fx, "open", "resolving")

	if err := orchestrator.HandleHandoffPRRebaseCompletion(ctx, pool, prID, true, 3); err != nil {
		t.Fatalf("HandleHandoffPRRebaseCompletion: %v", err)
	}

	if got := getHandoffPRConflictState(t, ctx, pool, prID); got != "resolved" {
		t.Errorf("conflict_state = %q, want resolved", got)
	}
	if got := getHandoffPRRebaseAttempts(t, ctx, pool, prID); got != 0 {
		t.Errorf("rebase_attempts = %d, want 0 (reset)", got)
	}
}

// TestHandleHandoffPRRebaseCompletion_FailureBelowCap verifies that a failed
// rebase below the cap retries (resolving → conflicted, counter incremented).
func TestHandleHandoffPRRebaseCompletion_FailureBelowCap(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	_, prID := insertHandoffAndPR(t, ctx, pool, fx, "open", "resolving")

	if err := orchestrator.HandleHandoffPRRebaseCompletion(ctx, pool, prID, false, 3); err != nil {
		t.Fatalf("HandleHandoffPRRebaseCompletion: %v", err)
	}

	if got := getHandoffPRConflictState(t, ctx, pool, prID); got != "conflicted" {
		t.Errorf("conflict_state = %q, want conflicted (retry)", got)
	}
	if got := getHandoffPRRebaseAttempts(t, ctx, pool, prID); got != 1 {
		t.Errorf("rebase_attempts = %d, want 1 (incremented)", got)
	}
}

// TestHandleHandoffPRRebaseCompletion_CapStaysConflicted verifies Path B:
// at cap, handoff PR stays conflicted (not blocked — no task to escalate).
func TestHandleHandoffPRRebaseCompletion_CapStaysConflicted(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	_, prID := insertHandoffAndPR(t, ctx, pool, fx, "open", "resolving")

	// Set rebase_attempts to cap-1 so next failure hits the cap.
	if _, err := pool.Exec(ctx, `UPDATE handoff_prs SET rebase_attempts=2 WHERE id=$1`, prID); err != nil {
		t.Fatalf("preset rebase_attempts: %v", err)
	}

	if err := orchestrator.HandleHandoffPRRebaseCompletion(ctx, pool, prID, false, 3); err != nil {
		t.Fatalf("HandleHandoffPRRebaseCompletion: %v", err)
	}

	if got := getHandoffPRConflictState(t, ctx, pool, prID); got != "conflicted" {
		t.Errorf("conflict_state = %q, want conflicted (Path B stays at cap)", got)
	}

	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM handoff_prs WHERE id=$1`, prID).Scan(&status); err != nil {
		t.Fatalf("read status: %v", err)
	}
	if status != "open" {
		t.Errorf("status = %q, want open (Path B must not block)", status)
	}
}

// --- CheckAndFinalizeHandoffs integration tests ---

// TestCheckAndFinalizeHandoffs_AllMerged_Finalizes is the end-to-end integration
// test for the all-done → handoff → finalize path.
// When all handoff PRs are merged, the handoff is finalized and feature → done.
// ghMerger is nil because mgmt_pr_url is NULL (no mgmt PR to merge in this test).
func TestCheckAndFinalizeHandoffs_AllMerged_Finalizes(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	setFeatureStatus(t, ctx, pool, fx, "in_handoff")

	handoffID, prID := insertHandoffAndPR(t, ctx, pool, fx, "open", "none")

	// Mark the PR as merged.
	if _, err := pool.Exec(ctx, `UPDATE handoff_prs SET status='merged' WHERE id=$1`, prID); err != nil {
		t.Fatalf("set pr merged: %v", err)
	}

	// ghMerger=nil because mgmt_pr_url is NULL in this handoff.
	if err := orchestrator.CheckAndFinalizeHandoffs(ctx, pool, nil, fx.workspaceID); err != nil {
		t.Fatalf("CheckAndFinalizeHandoffs: %v", err)
	}

	// Feature must be done.
	if got := getFeatureStatus(t, ctx, pool, fx); got != "done" {
		t.Errorf("feature_status = %q, want done", got)
	}

	// Handoff must be finalized.
	var handoffStatus string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM handoffs WHERE id=$1`, handoffID,
	).Scan(&handoffStatus); err != nil {
		t.Fatalf("read handoff status: %v", err)
	}
	if handoffStatus != "finalized" {
		t.Errorf("handoff status = %q, want finalized", handoffStatus)
	}
}

// TestCheckAndFinalizeHandoffs_SkippedCounted_Finalizes verifies that a
// skipped_no_branch PR does not block finalization.
func TestCheckAndFinalizeHandoffs_SkippedCounted_Finalizes(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	setFeatureStatus(t, ctx, pool, fx, "in_handoff")

	handoffID, mergedPRID := insertHandoffAndPR(t, ctx, pool, fx, "open", "none")

	// Insert a second PR row for the same handoff — skipped_no_branch.
	skippedPRID := uuid.New()
	if _, err := pool.Exec(ctx,
		`INSERT INTO handoff_prs (id, handoff_id, repo, status, conflict_state)
		 VALUES ($1, $2, 'other-repo', 'skipped_no_branch', 'none')`,
		skippedPRID, handoffID,
	); err != nil {
		t.Fatalf("insert skipped_no_branch pr: %v", err)
	}

	// Mark the real PR merged.
	if _, err := pool.Exec(ctx, `UPDATE handoff_prs SET status='merged' WHERE id=$1`, mergedPRID); err != nil {
		t.Fatalf("set pr merged: %v", err)
	}

	if err := orchestrator.CheckAndFinalizeHandoffs(ctx, pool, nil, fx.workspaceID); err != nil {
		t.Fatalf("CheckAndFinalizeHandoffs: %v", err)
	}

	if got := getFeatureStatus(t, ctx, pool, fx); got != "done" {
		t.Errorf("feature_status = %q, want done (skipped PR does not block)", got)
	}
}

// TestCheckAndFinalizeHandoffs_NotAllMerged_NoFinalize verifies that when some
// handoff PRs are still open, finalization does NOT occur.
func TestCheckAndFinalizeHandoffs_NotAllMerged_NoFinalize(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	setFeatureStatus(t, ctx, pool, fx, "in_handoff")
	insertHandoffAndPR(t, ctx, pool, fx, "open", "none") // PR is still open

	if err := orchestrator.CheckAndFinalizeHandoffs(ctx, pool, nil, fx.workspaceID); err != nil {
		t.Fatalf("CheckAndFinalizeHandoffs: %v", err)
	}

	if got := getFeatureStatus(t, ctx, pool, fx); got != "in_handoff" {
		t.Errorf("feature_status = %q, want in_handoff (unchanged)", got)
	}
}

// TestCheckAndFinalizeHandoffs_NoPRs_Finalizes verifies that a handoff with no
// handoff_prs rows finalizes immediately (all repos were skipped_no_branch).
func TestCheckAndFinalizeHandoffs_NoPRs_Finalizes(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	setFeatureStatus(t, ctx, pool, fx, "in_handoff")

	handoffID := uuid.New()
	if _, err := pool.Exec(ctx,
		`INSERT INTO handoffs (id, workspace_id, feature_id, status)
		 VALUES ($1, $2, $3, 'open')`,
		handoffID, fx.workspaceID, fx.featureID,
	); err != nil {
		t.Fatalf("insert handoff: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM handoffs WHERE id=$1`, handoffID)
	})

	if err := orchestrator.CheckAndFinalizeHandoffs(ctx, pool, nil, fx.workspaceID); err != nil {
		t.Fatalf("CheckAndFinalizeHandoffs: %v", err)
	}

	if got := getFeatureStatus(t, ctx, pool, fx); got != "done" {
		t.Errorf("feature_status = %q, want done (no PRs → instant finalize)", got)
	}
}
