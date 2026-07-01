package orchestrator_test

// reviewer_integration_test.go exercises the T7 DB-backed transitions —
// HandleNoVerdict and ClaimFix — using a real database.
// SetReviewing, SetReviewPassed, SetChangeRequested, SetReviewIncomplete are
// already exercised in transitions_test.go and are not re-tested here.

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/tiendv89/workflow-orchestrator/internal/orchestrator"
)

// TestHandleNoVerdict_BelowMax_SetsReviewIncomplete verifies that when
// review_incomplete_count < max, HandleNoVerdict transitions to review_incomplete.
func TestHandleNoVerdict_BelowMax_SetsReviewIncomplete(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "reviewing")

	ok, err := orchestrator.HandleNoVerdict(ctx, pool, fx.workspaceID, taskID, 2)
	if err != nil {
		t.Fatalf("HandleNoVerdict error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true for below-max case")
	}

	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "review_incomplete" {
		t.Errorf("status = %v, want review_incomplete", row.Status)
	}
	if row.ReviewIncompleteCount != 1 {
		t.Errorf("review_incomplete_count = %d, want 1", row.ReviewIncompleteCount)
	}
}

// TestHandleNoVerdict_AtMax_SetsBlocked verifies that when
// review_incomplete_count >= max, HandleNoVerdict transitions to blocked with
// blocked_reason="review_incomplete_exceeded".
func TestHandleNoVerdict_AtMax_SetsBlocked(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "reviewing")

	// Set count to max (2) so the next call must escalate to blocked.
	_, err := pool.Exec(ctx,
		`UPDATE workspace_tasks SET review_incomplete_count=2
		 WHERE workspace_id=$1 AND task_id=$2`,
		fx.workspaceID, taskID,
	)
	if err != nil {
		t.Fatalf("set review_incomplete_count: %v", err)
	}

	ok, err := orchestrator.HandleNoVerdict(ctx, pool, fx.workspaceID, taskID, 2)
	if err != nil {
		t.Fatalf("HandleNoVerdict error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true for at-max case")
	}

	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "blocked" {
		t.Errorf("status = %v, want blocked", row.Status)
	}
	if row.BlockedReason == nil || *row.BlockedReason != "review_incomplete_exceeded" {
		t.Errorf("blocked_reason = %v, want review_incomplete_exceeded", row.BlockedReason)
	}
	if row.BlockedFromStatus == nil || *row.BlockedFromStatus != "reviewing" {
		t.Errorf("blocked_from_status = %v, want reviewing", row.BlockedFromStatus)
	}
}

// TestHandleNoVerdict_NotReviewing_NoOp verifies that HandleNoVerdict is a
// no-op when the task is not in "reviewing" state.
func TestHandleNoVerdict_NotReviewing_NoOp(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_review")

	ok, err := orchestrator.HandleNoVerdict(ctx, pool, fx.workspaceID, taskID, 2)
	if err != nil {
		t.Fatalf("HandleNoVerdict error: %v", err)
	}
	if ok {
		t.Error("expected ok=false when task not in reviewing state")
	}

	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "in_review" {
		t.Errorf("status = %v, want in_review (unchanged)", row.Status)
	}
}

// TestHandleNoVerdict_RetryEscalate verifies the full retry→escalate sequence:
// two review_incomplete cycles (count 0→1, 1→2) then blocked on the third.
func TestHandleNoVerdict_RetryEscalate(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "reviewing")

	// Round 1: count=0 < max=2 → review_incomplete, count becomes 1.
	ok, err := orchestrator.HandleNoVerdict(ctx, pool, fx.workspaceID, taskID, 2)
	if err != nil || !ok {
		t.Fatalf("round 1: error=%v, ok=%v", err, ok)
	}
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "review_incomplete" {
		t.Fatalf("round 1: status=%v, want review_incomplete", row.Status)
	}
	if row.ReviewIncompleteCount != 1 {
		t.Fatalf("round 1: count=%d, want 1", row.ReviewIncompleteCount)
	}

	// Simulate re-dispatch → back to reviewing.
	if _, err := pool.Exec(ctx,
		`UPDATE workspace_tasks SET status='reviewing'
		 WHERE workspace_id=$1 AND task_id=$2`,
		fx.workspaceID, taskID,
	); err != nil {
		t.Fatalf("reset to reviewing: %v", err)
	}

	// Round 2: count=1 < max=2 → review_incomplete, count becomes 2.
	ok, err = orchestrator.HandleNoVerdict(ctx, pool, fx.workspaceID, taskID, 2)
	if err != nil || !ok {
		t.Fatalf("round 2: error=%v, ok=%v", err, ok)
	}
	row = getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "review_incomplete" {
		t.Fatalf("round 2: status=%v, want review_incomplete", row.Status)
	}
	if row.ReviewIncompleteCount != 2 {
		t.Fatalf("round 2: count=%d, want 2", row.ReviewIncompleteCount)
	}

	// Simulate re-dispatch → back to reviewing.
	if _, err := pool.Exec(ctx,
		`UPDATE workspace_tasks SET status='reviewing'
		 WHERE workspace_id=$1 AND task_id=$2`,
		fx.workspaceID, taskID,
	); err != nil {
		t.Fatalf("reset to reviewing: %v", err)
	}

	// Round 3: count=2 >= max=2 → blocked.
	ok, err = orchestrator.HandleNoVerdict(ctx, pool, fx.workspaceID, taskID, 2)
	if err != nil || !ok {
		t.Fatalf("round 3: error=%v, ok=%v", err, ok)
	}
	row = getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "blocked" {
		t.Fatalf("round 3: status=%v, want blocked", row.Status)
	}
	if row.BlockedReason == nil || *row.BlockedReason != "review_incomplete_exceeded" {
		t.Fatalf("round 3: blocked_reason=%v, want review_incomplete_exceeded", row.BlockedReason)
	}
}

// TestClaimFix_ChangeRequested_ClaimsInProgress verifies that ClaimFix
// transitions change_requested → in_progress with dispatch columns set.
func TestClaimFix_ChangeRequested_ClaimsInProgress(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "change_requested")

	handle := uuid.New().String()
	nonce := uuid.New().String()

	ok, err := orchestrator.ClaimFix(ctx, pool, fx.workspaceID, taskID, handle, nonce)
	if err != nil {
		t.Fatalf("ClaimFix error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true for change_requested task")
	}

	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "in_progress" {
		t.Errorf("status = %v, want in_progress", row.Status)
	}
	if row.DispatchHandle == nil || *row.DispatchHandle != handle {
		t.Errorf("dispatch_handle = %v, want %q", row.DispatchHandle, handle)
	}
	if row.DispatchNonce == nil || *row.DispatchNonce != nonce {
		t.Errorf("dispatch_nonce = %v, want %q", row.DispatchNonce, nonce)
	}
	if row.DispatchKind == nil || *row.DispatchKind != "fix" {
		t.Errorf("dispatch_kind = %v, want fix", row.DispatchKind)
	}
	if row.ReenqueueAttempts != 0 {
		t.Errorf("reenqueue_attempts = %d, want 0 (reset on dispatch-in)", row.ReenqueueAttempts)
	}
}

// TestClaimFix_WrongStatus_NoOp verifies that ClaimFix is a no-op when the
// task is not in change_requested status.
func TestClaimFix_WrongStatus_NoOp(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_review")

	ok, err := orchestrator.ClaimFix(ctx, pool, fx.workspaceID, taskID, "handle", "nonce")
	if err != nil {
		t.Fatalf("ClaimFix error: %v", err)
	}
	if ok {
		t.Error("expected ok=false for wrong precondition")
	}

	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "in_review" {
		t.Errorf("status = %v, want in_review (unchanged)", row.Status)
	}
}

// TestClaimFix_FirstWriteWins verifies that concurrent ClaimFix calls on the
// same change_requested task result in exactly one winner.
func TestClaimFix_FirstWriteWins(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "change_requested")

	var mu sync.Mutex
	wins := 0
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := orchestrator.ClaimFix(ctx, pool, fx.workspaceID, taskID, uuid.New().String(), uuid.New().String())
			if err == nil && ok {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if wins != 1 {
		t.Errorf("concurrent ClaimFix: %d winners, want exactly 1", wins)
	}
}
