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

func TestGuardedTransition_AnyFromStatus(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_progress")

	ok, err := orchestrator.GuardedTransition(ctx, pool, fx.workspaceID, taskID, "*", "blocked", map[string]any{"blocked_reason": "test"})
	if err != nil {
		t.Fatalf("GuardedTransition returned error: %v", err)
	}
	if !ok {
		t.Error("expected ok=true for any-status transition")
	}
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != "blocked" {
		t.Errorf("expected status=blocked, got %v", row.Status)
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
