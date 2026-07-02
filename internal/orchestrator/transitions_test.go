package orchestrator_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tiendv89/workflow-orchestrator/internal/database/queries"
	"github.com/tiendv89/workflow-orchestrator/internal/orchestrator"
)

// insertTaskWithStatus inserts a task with the given status and returns its task_id.
func insertTaskWithStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fx testFixture, status string) uuid.UUID {
	t.Helper()
	q := queries.New(pool)
	taskID := uuid.New()
	taskName := "T-" + taskID.String()[:8]
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
		t.Fatalf("insertTaskWithStatus: %v", err)
	}
	return taskID
}

// getTaskRow fetches the full task row for assertion.
func getTaskRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fx testFixture, taskID uuid.UUID) queries.WorkspaceTask {
	t.Helper()
	q := queries.New(pool)
	row, err := q.GetTaskByUUID(ctx, queries.GetTaskByUUIDParams{
		WorkspaceID: fx.workspaceID,
		TaskID:      taskID,
	})
	if err != nil {
		t.Fatalf("getTaskRow: %v", err)
	}
	return row
}

func TestGuardedTransition_MatchingStatus(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "ready")

	ok, err := orchestrator.GuardedTransition(ctx, pool, fx.workspaceID, taskID, "ready", "in_progress", nil)
	if err != nil {
		t.Fatalf("GuardedTransition returned error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true for matching status")
	}
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "in_progress" {
		t.Errorf("expected status=in_progress, got %v", row.Status)
	}
}

func TestGuardedTransition_WrongStatus(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "ready")

	ok, err := orchestrator.GuardedTransition(ctx, pool, fx.workspaceID, taskID, "in_progress", "done", nil)
	if err != nil {
		t.Fatalf("GuardedTransition returned unexpected error: %v", err)
	}
	if ok {
		t.Error("expected ok=false for wrong precondition status")
	}
	// Status must remain unchanged.
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "ready" {
		t.Errorf("expected status=ready (unchanged), got %v", row.Status)
	}
}

func TestGuardedTransition_AnyFromStatus_NonTerminal(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_progress")

	ok, err := orchestrator.GuardedTransition(ctx, pool, fx.workspaceID, taskID, "*", "blocked", map[string]any{"blocked_reason": "test"})
	if err != nil {
		t.Fatalf("GuardedTransition returned error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true for any-status transition on non-terminal task")
	}
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "blocked" {
		t.Errorf("expected status=blocked, got %v", row.Status)
	}
}

func TestGuardedTransition_AnyFromStatus_SkipsTerminal(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	for _, terminal := range []string{"done", "cancelled"} {
		taskID := insertTaskWithStatus(t, ctx, pool, fx, terminal)
		ok, err := orchestrator.GuardedTransition(ctx, pool, fx.workspaceID, taskID, "*", "blocked", map[string]any{"blocked_reason": "late-arrival"})
		if err != nil {
			t.Fatalf("GuardedTransition(%s→blocked): error: %v", terminal, err)
		}
		if ok {
			t.Errorf("expected ok=false for wildcard transition on terminal status=%s", terminal)
		}
		row := getTaskRow(t, ctx, pool, fx, taskID)
		if row.Status == nil || *row.Status != terminal {
			t.Errorf("status should remain %s, got %v", terminal, row.Status)
		}
	}
}

func TestSetInReview_Success(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_progress")

	ok, err := orchestrator.SetInReview(ctx, pool, fx.workspaceID, taskID, "https://github.com/example/pr/1")
	if err != nil {
		t.Fatalf("SetInReview returned error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "in_review" {
		t.Errorf("expected status=in_review, got %v", row.Status)
	}

	var pr map[string]string
	if err := json.Unmarshal(row.Pr, &pr); err != nil {
		t.Fatalf("unmarshal pr field: %v", err)
	}
	if pr["url"] != "https://github.com/example/pr/1" {
		t.Errorf("expected pr.url=https://github.com/example/pr/1, got %q", pr["url"])
	}
	if pr["status"] != "open" {
		t.Errorf("expected pr.status=open, got %q", pr["status"])
	}

	// dispatch columns should be cleared
	if row.DispatchHandle != nil {
		t.Errorf("expected dispatch_handle=nil after SetInReview, got %v", *row.DispatchHandle)
	}
	if row.DispatchNonce != nil {
		t.Errorf("expected dispatch_nonce=nil after SetInReview, got %v", *row.DispatchNonce)
	}

	// max_turns_retry_count must be reset to 0 on every successful completion
	if row.MaxTurnsRetryCount != 0 {
		t.Errorf("expected max_turns_retry_count=0 after SetInReview, got %d", row.MaxTurnsRetryCount)
	}
}

// TestSetInReview_ResetsMaxTurnsCounter verifies the reset contract when
// the task has accumulated max-turns retries from prior execution attempts.
func TestSetInReview_ResetsMaxTurnsCounter(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_progress")

	// Simulate two prior max-turns events by directly setting the counter.
	_, err := pool.Exec(ctx,
		`UPDATE workspace_tasks SET max_turns_retry_count = 2
		 WHERE workspace_id = $1 AND task_id = $2`,
		fx.workspaceID, taskID,
	)
	if err != nil {
		t.Fatalf("setup max_turns_retry_count: %v", err)
	}

	ok, err := orchestrator.SetInReview(ctx, pool, fx.workspaceID, taskID, "https://github.com/example/pr/3")
	if err != nil {
		t.Fatalf("SetInReview returned error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.MaxTurnsRetryCount != 0 {
		t.Errorf("expected max_turns_retry_count reset to 0 after successful completion, got %d", row.MaxTurnsRetryCount)
	}
}

func TestSetInReview_WrongPrecondition(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "ready") // not in_progress

	ok, err := orchestrator.SetInReview(ctx, pool, fx.workspaceID, taskID, "https://github.com/example/pr/2")
	if err != nil {
		t.Fatalf("SetInReview returned unexpected error: %v", err)
	}
	if ok {
		t.Error("expected ok=false for wrong precondition")
	}
}

func TestSetBlocked_SetsReasonAndStatus(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_progress")

	ok, err := orchestrator.SetBlocked(ctx, pool, fx.workspaceID, taskID, "dependency_missing")
	if err != nil {
		t.Fatalf("SetBlocked returned error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "blocked" {
		t.Errorf("expected status=blocked, got %v", row.Status)
	}
	if row.BlockedReason == nil || *row.BlockedReason != "dependency_missing" {
		t.Errorf("expected blocked_reason=dependency_missing, got %v", row.BlockedReason)
	}
}

func TestSetBlocked_FromAnyStatus(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	for _, status := range []string{"ready", "in_progress", "in_review"} {
		taskID := insertTaskWithStatus(t, ctx, pool, fx, status)
		ok, err := orchestrator.SetBlocked(ctx, pool, fx.workspaceID, taskID, "test")
		if err != nil {
			t.Fatalf("SetBlocked from %s returned error: %v", status, err)
		}
		if !ok {
			t.Errorf("expected ok=true when blocking from status=%s", status)
		}
	}
}

func TestSetBlocked_DoesNotBlockTerminal(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	for _, terminal := range []string{"done", "cancelled"} {
		taskID := insertTaskWithStatus(t, ctx, pool, fx, terminal)
		ok, err := orchestrator.SetBlocked(ctx, pool, fx.workspaceID, taskID, "late-failure")
		if err != nil {
			t.Fatalf("SetBlocked on terminal(%s) returned error: %v", terminal, err)
		}
		if ok {
			t.Errorf("expected ok=false when SetBlocked applied to terminal status=%s", terminal)
		}
		row := getTaskRow(t, ctx, pool, fx, taskID)
		if row.Status == nil || *row.Status != terminal {
			t.Errorf("terminal status=%s must not change, got %v", terminal, row.Status)
		}
	}
}

func TestSetBlockedWithDetails_RecordsDetails(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "reviewing")

	ok, err := orchestrator.SetBlockedWithDetails(ctx, pool, fx.workspaceID, taskID, "reconciler_max", "Reconciler hit DISPATCH_RECONCILE_MAX_RETRIES=3", "reviewing")
	if err != nil {
		t.Fatalf("SetBlockedWithDetails returned error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "blocked" {
		t.Errorf("expected status=blocked, got %v", row.Status)
	}
	if row.BlockedReason == nil || *row.BlockedReason != "reconciler_max" {
		t.Errorf("expected blocked_reason=reconciler_max, got %v", row.BlockedReason)
	}
	if row.BlockedDetails == nil || *row.BlockedDetails != "Reconciler hit DISPATCH_RECONCILE_MAX_RETRIES=3" {
		t.Errorf("expected blocked_details set, got %v", row.BlockedDetails)
	}
	if row.BlockedFromStatus == nil || *row.BlockedFromStatus != "reviewing" {
		t.Errorf("expected blocked_from_status=reviewing, got %v", row.BlockedFromStatus)
	}
}

func TestSetReviewing_FromInReview(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_review")

	ok, err := orchestrator.SetReviewing(ctx, pool, fx.workspaceID, taskID, "in_review", "handle-abc", "nonce-xyz")
	if err != nil {
		t.Fatalf("SetReviewing returned error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "reviewing" {
		t.Errorf("expected status=reviewing, got %v", row.Status)
	}
	if row.DispatchHandle == nil || *row.DispatchHandle != "handle-abc" {
		t.Errorf("expected dispatch_handle=handle-abc, got %v", row.DispatchHandle)
	}
	if row.DispatchNonce == nil || *row.DispatchNonce != "nonce-xyz" {
		t.Errorf("expected dispatch_nonce=nonce-xyz, got %v", row.DispatchNonce)
	}
	if row.DispatchKind == nil || *row.DispatchKind != "review" {
		t.Errorf("expected dispatch_kind=review, got %v", row.DispatchKind)
	}
	if !row.DispatchedAt.Valid {
		t.Error("expected dispatched_at to be set")
	}
	if row.ReenqueueAttempts != 0 {
		t.Errorf("expected reenqueue_attempts=0 on dispatch-in, got %d", row.ReenqueueAttempts)
	}
}

func TestSetReviewing_FromReviewIncomplete(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "review_incomplete")

	ok, err := orchestrator.SetReviewing(ctx, pool, fx.workspaceID, taskID, "review_incomplete", "h2", "n2")
	if err != nil {
		t.Fatalf("SetReviewing from review_incomplete: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "reviewing" {
		t.Errorf("expected status=reviewing, got %v", row.Status)
	}
}

func TestSetReviewing_WrongPrecondition(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_progress")

	ok, err := orchestrator.SetReviewing(ctx, pool, fx.workspaceID, taskID, "in_review", "h", "n")
	if err != nil {
		t.Fatalf("SetReviewing returned unexpected error: %v", err)
	}
	if ok {
		t.Error("expected ok=false for wrong precondition")
	}
}

func TestSetReviewPassed_Success(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "reviewing")

	ok, err := orchestrator.SetReviewPassed(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("SetReviewPassed returned error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "review_passed" {
		t.Errorf("expected status=review_passed, got %v", row.Status)
	}
	// dispatch columns should be cleared
	if row.DispatchHandle != nil {
		t.Errorf("expected dispatch_handle=nil after SetReviewPassed, got %v", *row.DispatchHandle)
	}
}

func TestSetReviewPassed_WrongPrecondition(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_review")

	ok, err := orchestrator.SetReviewPassed(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("SetReviewPassed returned unexpected error: %v", err)
	}
	if ok {
		t.Error("expected ok=false for wrong precondition (in_review, not reviewing)")
	}
}

func TestSetChangeRequested_Success(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "reviewing")

	ok, err := orchestrator.SetChangeRequested(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("SetChangeRequested returned error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "change_requested" {
		t.Errorf("expected status=change_requested, got %v", row.Status)
	}
	if row.DispatchHandle != nil {
		t.Errorf("expected dispatch_handle cleared, got %v", *row.DispatchHandle)
	}
}

func TestSetReviewIncomplete_Success(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "reviewing")

	ok, err := orchestrator.SetReviewIncomplete(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("SetReviewIncomplete returned error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "review_incomplete" {
		t.Errorf("expected status=review_incomplete, got %v", row.Status)
	}
	if row.ReviewIncompleteCount != 1 {
		t.Errorf("expected review_incomplete_count=1, got %d", row.ReviewIncompleteCount)
	}
	if row.DispatchHandle != nil {
		t.Errorf("expected dispatch_handle cleared, got %v", *row.DispatchHandle)
	}
}

func TestSetReviewIncomplete_Accumulates(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	// Simulate two review_incomplete cycles.
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "reviewing")
	if _, err := orchestrator.SetReviewIncomplete(ctx, pool, fx.workspaceID, taskID); err != nil {
		t.Fatal(err)
	}
	// Reset to reviewing for a second round.
	if _, err := orchestrator.SetReviewing(ctx, pool, fx.workspaceID, taskID, "review_incomplete", "h", "n"); err != nil {
		t.Fatal(err)
	}
	if _, err := orchestrator.SetReviewIncomplete(ctx, pool, fx.workspaceID, taskID); err != nil {
		t.Fatal(err)
	}

	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.ReviewIncompleteCount != 2 {
		t.Errorf("expected review_incomplete_count=2 after two cycles, got %d", row.ReviewIncompleteCount)
	}
}

func TestSetReviewIncomplete_WrongPrecondition(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_review")

	ok, err := orchestrator.SetReviewIncomplete(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("SetReviewIncomplete returned unexpected error: %v", err)
	}
	if ok {
		t.Error("expected ok=false for wrong precondition")
	}
}

func TestSetDoneFromMergedPR_FromInReview(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_review")

	ok, err := orchestrator.SetDoneFromMergedPR(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("SetDoneFromMergedPR returned error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true from in_review")
	}
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "done" {
		t.Errorf("expected status=done, got %v", row.Status)
	}
}

func TestSetDoneFromMergedPR_FromReviewing(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "reviewing")

	ok, err := orchestrator.SetDoneFromMergedPR(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("SetDoneFromMergedPR returned error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true from reviewing")
	}
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "done" {
		t.Errorf("expected status=done, got %v", row.Status)
	}
}

func TestSetDoneFromMergedPR_FromReviewPassed(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "review_passed")

	ok, err := orchestrator.SetDoneFromMergedPR(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("SetDoneFromMergedPR returned error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true from review_passed")
	}
}

func TestSetDoneFromMergedPR_WrongPrecondition(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_progress")

	ok, err := orchestrator.SetDoneFromMergedPR(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("SetDoneFromMergedPR returned unexpected error: %v", err)
	}
	if ok {
		t.Error("expected ok=false for wrong precondition (in_progress)")
	}
}

func TestSetDone_Success(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_review")

	ok, err := orchestrator.SetDone(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("SetDone returned error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "done" {
		t.Errorf("expected status=done, got %v", row.Status)
	}
}

func TestSetDone_WrongPrecondition(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_progress") // not in_review

	ok, err := orchestrator.SetDone(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("SetDone returned unexpected error: %v", err)
	}
	if ok {
		t.Error("expected ok=false for wrong precondition")
	}
}

func TestSetReadyFromMaxTurns_Success(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_progress")

	ok, err := orchestrator.SetReadyFromMaxTurns(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("SetReadyFromMaxTurns returned error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "ready" {
		t.Errorf("expected status=ready, got %v", row.Status)
	}
	if row.MaxTurnsRetryCount != 1 {
		t.Errorf("expected max_turns_retry_count=1 after first retry, got %d", row.MaxTurnsRetryCount)
	}
	// dispatch columns should be cleared
	if row.DispatchHandle != nil {
		t.Errorf("expected dispatch_handle=nil, got %v", *row.DispatchHandle)
	}
	if row.DispatchNonce != nil {
		t.Errorf("expected dispatch_nonce=nil, got %v", *row.DispatchNonce)
	}
}

func TestSetReadyFromMaxTurns_Accumulates(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_progress")

	// Two max-turns resets: counter should reach 2.
	if _, err := orchestrator.SetReadyFromMaxTurns(ctx, pool, fx.workspaceID, taskID); err != nil {
		t.Fatalf("first SetReadyFromMaxTurns: %v", err)
	}
	// Re-claim (ready → in_progress) to allow a second reset.
	if _, err := orchestrator.GuardedTransition(ctx, pool, fx.workspaceID, taskID, "ready", "in_progress", nil); err != nil {
		t.Fatalf("re-claim: %v", err)
	}
	if _, err := orchestrator.SetReadyFromMaxTurns(ctx, pool, fx.workspaceID, taskID); err != nil {
		t.Fatalf("second SetReadyFromMaxTurns: %v", err)
	}

	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.MaxTurnsRetryCount != 2 {
		t.Errorf("expected max_turns_retry_count=2 after two resets, got %d", row.MaxTurnsRetryCount)
	}
}

func TestSetReadyFromMaxTurns_WrongPrecondition(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "ready") // not in_progress

	ok, err := orchestrator.SetReadyFromMaxTurns(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("SetReadyFromMaxTurns returned unexpected error: %v", err)
	}
	if ok {
		t.Error("expected ok=false for wrong precondition")
	}
}

func TestBumpReenqueueAttempts_Increments(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_progress")

	count1, err := orchestrator.BumpReenqueueAttempts(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("BumpReenqueueAttempts (1st): %v", err)
	}
	if count1 != 1 {
		t.Errorf("expected count=1 after first bump, got %d", count1)
	}

	count2, err := orchestrator.BumpReenqueueAttempts(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("BumpReenqueueAttempts (2nd): %v", err)
	}
	if count2 != 2 {
		t.Errorf("expected count=2 after second bump, got %d", count2)
	}

	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.ReenqueueAttempts != 2 {
		t.Errorf("expected reenqueue_attempts=2 in DB, got %d", row.ReenqueueAttempts)
	}
}

func TestGetMaxTurnsRetryCount_ReturnsCurrentCount(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_progress")

	count, err := orchestrator.GetMaxTurnsRetryCount(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("GetMaxTurnsRetryCount: %v", err)
	}
	if count != 0 {
		t.Errorf("expected initial count=0, got %d", count)
	}

	// Bump via SetReadyFromMaxTurns and re-claim to verify count reads back correctly.
	if _, err := orchestrator.SetReadyFromMaxTurns(ctx, pool, fx.workspaceID, taskID); err != nil {
		t.Fatalf("SetReadyFromMaxTurns: %v", err)
	}
	if _, err := orchestrator.GuardedTransition(ctx, pool, fx.workspaceID, taskID, "ready", "in_progress", nil); err != nil {
		t.Fatalf("re-claim: %v", err)
	}

	count, err = orchestrator.GetMaxTurnsRetryCount(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("GetMaxTurnsRetryCount after bump: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count=1 after one reset, got %d", count)
	}
}
