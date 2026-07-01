package orchestrator_test

// Tests for blocked_from_status recording on every →blocked path and for the
// resume loops that pick up unblocked tasks (ready→claim, in_review→reviewer dispatch).

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tiendv89/workflow-orchestrator/internal/database/queries"
	"github.com/tiendv89/workflow-orchestrator/internal/orchestrator"
)

// insertTaskWithStatusAndFeature inserts a task with the given status under a known feature.
func insertTaskWithStatusAndFeature(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	fx testFixture,
	status string,
) uuid.UUID {
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
		t.Fatalf("insertTaskWithStatusAndFeature: %v", err)
	}
	return taskID
}

// setTaskStatus updates the task status directly for test setup.
func setTaskStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, workspaceID, taskID uuid.UUID, status string) {
	t.Helper()
	_, err := pool.Exec(ctx,
		`UPDATE workspace_tasks SET status=$1, updated_at=now() WHERE workspace_id=$2 AND task_id=$3`,
		status, workspaceID, taskID,
	)
	if err != nil {
		t.Fatalf("setTaskStatus(%q): %v", status, err)
	}
}

// setTaskPR updates the pr column to a minimal open-PR JSON for test setup.
func setTaskPR(t *testing.T, ctx context.Context, pool *pgxpool.Pool, workspaceID, taskID uuid.UUID, prURL string) {
	t.Helper()
	pr, _ := json.Marshal(map[string]string{"url": prURL, "status": "open"})
	_, err := pool.Exec(ctx,
		`UPDATE workspace_tasks SET pr=$1::jsonb, updated_at=now() WHERE workspace_id=$2 AND task_id=$3`,
		pr, workspaceID, taskID,
	)
	if err != nil {
		t.Fatalf("setTaskPR: %v", err)
	}
}

// --- blocked_from_status recording tests ---

// TestSetBlockedWithDetails_ImplBlock verifies that blocking a task that was in
// "in_progress" records blocked_from_status="in_progress".
func TestSetBlockedWithDetails_ImplBlock(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatusAndFeature(t, ctx, pool, fx, "in_progress")

	ok, err := orchestrator.SetBlockedWithDetails(ctx, pool, fx.workspaceID, taskID, "tests_failed", "", "in_progress")
	if err != nil {
		t.Fatalf("SetBlockedWithDetails: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}

	q := queries.New(pool)
	row, err := q.GetTaskByUUID(ctx, queries.GetTaskByUUIDParams{WorkspaceID: fx.workspaceID, TaskID: taskID})
	if err != nil {
		t.Fatalf("GetTaskByUUID: %v", err)
	}
	if row.Status == nil || *row.Status != "blocked" {
		t.Errorf("status = %v, want blocked", row.Status)
	}
	if row.BlockedFromStatus == nil || *row.BlockedFromStatus != "in_progress" {
		t.Errorf("blocked_from_status = %v, want in_progress", row.BlockedFromStatus)
	}
}

// TestSetBlockedWithDetails_ReviewBlock verifies that blocking a task that was in
// "reviewing" records blocked_from_status="reviewing".
func TestSetBlockedWithDetails_ReviewBlock(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatusAndFeature(t, ctx, pool, fx, "reviewing")

	ok, err := orchestrator.SetBlockedWithDetails(ctx, pool, fx.workspaceID, taskID, "missing_tool", "", "reviewing")
	if err != nil {
		t.Fatalf("SetBlockedWithDetails: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}

	q := queries.New(pool)
	row, err := q.GetTaskByUUID(ctx, queries.GetTaskByUUIDParams{WorkspaceID: fx.workspaceID, TaskID: taskID})
	if err != nil {
		t.Fatalf("GetTaskByUUID: %v", err)
	}
	if row.BlockedFromStatus == nil || *row.BlockedFromStatus != "reviewing" {
		t.Errorf("blocked_from_status = %v, want reviewing", row.BlockedFromStatus)
	}
}

// TestSetBlockedWithDetails_ReviewIncompleteCapBlock verifies that blocking a task
// from "review_incomplete" (cap exceeded) records blocked_from_status="review_incomplete".
func TestSetBlockedWithDetails_ReviewIncompleteCapBlock(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatusAndFeature(t, ctx, pool, fx, "review_incomplete")

	ok, err := orchestrator.SetBlockedWithDetails(ctx, pool, fx.workspaceID, taskID,
		"review_incomplete_max", "Exceeded MAX_REVIEW_INCOMPLETES", "review_incomplete")
	if err != nil {
		t.Fatalf("SetBlockedWithDetails: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}

	q := queries.New(pool)
	row, err := q.GetTaskByUUID(ctx, queries.GetTaskByUUIDParams{WorkspaceID: fx.workspaceID, TaskID: taskID})
	if err != nil {
		t.Fatalf("GetTaskByUUID: %v", err)
	}
	if row.BlockedFromStatus == nil || *row.BlockedFromStatus != "review_incomplete" {
		t.Errorf("blocked_from_status = %v, want review_incomplete", row.BlockedFromStatus)
	}
	if row.BlockedDetails == nil || *row.BlockedDetails != "Exceeded MAX_REVIEW_INCOMPLETES" {
		t.Errorf("blocked_details = %v, want explanation string", row.BlockedDetails)
	}
}

// TestSetBlockedWithDetails_MaxTurnsCapBlock verifies that blocking a task from
// "in_progress" due to max-turns cap records blocked_from_status="in_progress".
func TestSetBlockedWithDetails_MaxTurnsCapBlock(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatusAndFeature(t, ctx, pool, fx, "in_progress")

	ok, err := orchestrator.SetBlockedWithDetails(ctx, pool, fx.workspaceID, taskID,
		"max_turns_exceeded", "Exceeded EXECUTOR_MAX_RETRIES", "in_progress")
	if err != nil {
		t.Fatalf("SetBlockedWithDetails: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}

	q := queries.New(pool)
	row, err := q.GetTaskByUUID(ctx, queries.GetTaskByUUIDParams{WorkspaceID: fx.workspaceID, TaskID: taskID})
	if err != nil {
		t.Fatalf("GetTaskByUUID: %v", err)
	}
	if row.BlockedFromStatus == nil || *row.BlockedFromStatus != "in_progress" {
		t.Errorf("blocked_from_status = %v, want in_progress", row.BlockedFromStatus)
	}
}

// TestSetBlockedWithDetails_InReviewBlock verifies that blocking a task from
// "in_review" (e.g. rebase cap Path A) records blocked_from_status="in_review".
func TestSetBlockedWithDetails_InReviewBlock(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatusAndFeature(t, ctx, pool, fx, "in_review")

	ok, err := orchestrator.SetBlockedWithDetails(ctx, pool, fx.workspaceID, taskID,
		"rebase_cap", "Exceeded rebase attempt cap", "in_review")
	if err != nil {
		t.Fatalf("SetBlockedWithDetails: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}

	q := queries.New(pool)
	row, err := q.GetTaskByUUID(ctx, queries.GetTaskByUUIDParams{WorkspaceID: fx.workspaceID, TaskID: taskID})
	if err != nil {
		t.Fatalf("GetTaskByUUID: %v", err)
	}
	if row.BlockedFromStatus == nil || *row.BlockedFromStatus != "in_review" {
		t.Errorf("blocked_from_status = %v, want in_review", row.BlockedFromStatus)
	}
}

// --- resume path tests ---

// TestResumePath_ReadyReentersClaim verifies that a task unblocked back to "ready"
// is returned by FindEligibleTasks and therefore re-enters the claim loop.
func TestResumePath_ReadyReentersClaim(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatusAndFeature(t, ctx, pool, fx, "in_progress")

	// Block from in_progress.
	if _, err := orchestrator.SetBlockedWithDetails(ctx, pool, fx.workspaceID, taskID,
		"tests_failed", "", "in_progress"); err != nil {
		t.Fatalf("SetBlockedWithDetails: %v", err)
	}

	// Human unblocks: backend API sets status → ready.
	setTaskStatus(t, ctx, pool, fx.workspaceID, taskID, "ready")

	// Claim loop should now see the task as eligible.
	got, err := orchestrator.FindEligibleTasks(ctx, pool, fx.workspaceID)
	if err != nil {
		t.Fatalf("FindEligibleTasks: %v", err)
	}
	found := false
	for _, task := range got {
		if task.TaskID == taskID {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected unblocked task (reset to ready) to appear in FindEligibleTasks, but it did not")
	}
}

// TestResumePath_InReviewReentersReviewerDispatch verifies that a task unblocked
// back to "in_review" (after being blocked from "reviewing") is returned by
// FindReviewableTasks and therefore re-enters the reviewer dispatch loop.
func TestResumePath_InReviewReentersReviewerDispatch(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatusAndFeature(t, ctx, pool, fx, "reviewing")

	// Block from reviewing.
	if _, err := orchestrator.SetBlockedWithDetails(ctx, pool, fx.workspaceID, taskID,
		"missing_tool", "", "reviewing"); err != nil {
		t.Fatalf("SetBlockedWithDetails: %v", err)
	}

	// Human unblocks: backend API sets status → in_review (derived from reviewing).
	setTaskStatus(t, ctx, pool, fx.workspaceID, taskID, "in_review")
	setTaskPR(t, ctx, pool, fx.workspaceID, taskID, "https://github.com/owner/repo/pull/42")

	// Reviewer dispatch loop should now see the task.
	got, err := orchestrator.FindReviewableTasks(ctx, pool, fx.workspaceID)
	if err != nil {
		t.Fatalf("FindReviewableTasks: %v", err)
	}
	found := false
	for _, task := range got {
		if task.TaskID == taskID {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected unblocked task (reset to in_review) to appear in FindReviewableTasks, but it did not")
	}
}

// TestResumePath_BlockedNotEligible verifies that a blocked task is excluded from
// both the claim loop (FindEligibleTasks) and the reviewer dispatch loop (FindReviewableTasks).
func TestResumePath_BlockedNotEligible(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	// Create two tasks: one blocked from in_progress, one blocked from reviewing.
	taskFromInProgress := insertTaskWithStatusAndFeature(t, ctx, pool, fx, "in_progress")
	taskFromReviewing := insertTaskWithStatusAndFeature(t, ctx, pool, fx, "reviewing")

	if _, err := orchestrator.SetBlockedWithDetails(ctx, pool, fx.workspaceID, taskFromInProgress,
		"tests_failed", "", "in_progress"); err != nil {
		t.Fatalf("SetBlockedWithDetails(in_progress): %v", err)
	}
	if _, err := orchestrator.SetBlockedWithDetails(ctx, pool, fx.workspaceID, taskFromReviewing,
		"missing_tool", "", "reviewing"); err != nil {
		t.Fatalf("SetBlockedWithDetails(reviewing): %v", err)
	}

	eligible, err := orchestrator.FindEligibleTasks(ctx, pool, fx.workspaceID)
	if err != nil {
		t.Fatalf("FindEligibleTasks: %v", err)
	}
	for _, task := range eligible {
		if task.TaskID == taskFromInProgress || task.TaskID == taskFromReviewing {
			t.Errorf("blocked task %v should not appear in FindEligibleTasks", task.TaskID)
		}
	}

	reviewable, err := orchestrator.FindReviewableTasks(ctx, pool, fx.workspaceID)
	if err != nil {
		t.Fatalf("FindReviewableTasks: %v", err)
	}
	for _, task := range reviewable {
		if task.TaskID == taskFromInProgress || task.TaskID == taskFromReviewing {
			t.Errorf("blocked task %v should not appear in FindReviewableTasks", task.TaskID)
		}
	}
}

// TestFindReviewableTasks_ReturnsInReviewAndReviewIncomplete verifies that
// FindReviewableTasks returns tasks in both "in_review" and "review_incomplete"
// status when they have a PR URL set.
func TestFindReviewableTasks_ReturnsInReviewAndReviewIncomplete(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	// in_review task with PR.
	taskInReview := insertTaskWithStatusAndFeature(t, ctx, pool, fx, "in_review")
	setTaskPR(t, ctx, pool, fx.workspaceID, taskInReview, "https://github.com/owner/repo/pull/1")

	// review_incomplete task with PR.
	taskReviewIncomplete := insertTaskWithStatusAndFeature(t, ctx, pool, fx, "review_incomplete")
	setTaskPR(t, ctx, pool, fx.workspaceID, taskReviewIncomplete, "https://github.com/owner/repo/pull/2")

	// in_progress task — must be excluded.
	taskInProgress := insertTaskWithStatusAndFeature(t, ctx, pool, fx, "in_progress")

	got, err := orchestrator.FindReviewableTasks(ctx, pool, fx.workspaceID)
	if err != nil {
		t.Fatalf("FindReviewableTasks: %v", err)
	}

	gotIDs := make(map[uuid.UUID]bool, len(got))
	for _, task := range got {
		gotIDs[task.TaskID] = true
	}

	if !gotIDs[taskInReview] {
		t.Error("expected in_review task with PR to appear in FindReviewableTasks")
	}
	if !gotIDs[taskReviewIncomplete] {
		t.Error("expected review_incomplete task with PR to appear in FindReviewableTasks")
	}
	if gotIDs[taskInProgress] {
		t.Error("in_progress task must not appear in FindReviewableTasks")
	}
}

// TestFindReviewableTasks_ExcludesTasksWithoutPR verifies that in_review tasks
// without a PR URL are not returned (they haven't reached PR stage yet).
func TestFindReviewableTasks_ExcludesTasksWithoutPR(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	// in_review task WITHOUT PR.
	taskNoPR := insertTaskWithStatusAndFeature(t, ctx, pool, fx, "in_review")

	got, err := orchestrator.FindReviewableTasks(ctx, pool, fx.workspaceID)
	if err != nil {
		t.Fatalf("FindReviewableTasks: %v", err)
	}
	for _, task := range got {
		if task.TaskID == taskNoPR {
			t.Error("in_review task without PR URL must not appear in FindReviewableTasks")
		}
	}
}

// TestBlockedFromStatus_PreservedThroughUnblock verifies that blocked_from_status
// is preserved in the DB row and readable after block, so the unblock API can use it.
func TestBlockedFromStatus_PreservedThroughUnblock(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatusAndFeature(t, ctx, pool, fx, "in_progress")

	if _, err := orchestrator.SetBlockedWithDetails(ctx, pool, fx.workspaceID, taskID,
		"tests_failed", "tests failed on attempt 3", "in_progress"); err != nil {
		t.Fatalf("SetBlockedWithDetails: %v", err)
	}

	// Verify blocked_from_status is persisted and readable.
	q := queries.New(pool)
	row, err := q.GetTaskByUUID(ctx, queries.GetTaskByUUIDParams{
		WorkspaceID: fx.workspaceID,
		TaskID:      taskID,
	})
	if err != nil {
		t.Fatalf("GetTaskByUUID: %v", err)
	}
	if row.BlockedFromStatus == nil {
		t.Fatal("blocked_from_status is nil; the unblock API cannot determine resume state")
	}
	if *row.BlockedFromStatus != "in_progress" {
		t.Errorf("blocked_from_status = %q, want in_progress", *row.BlockedFromStatus)
	}
	if row.BlockedReason == nil || *row.BlockedReason != "tests_failed" {
		t.Errorf("blocked_reason = %v, want tests_failed", row.BlockedReason)
	}
}
