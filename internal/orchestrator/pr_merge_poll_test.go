package orchestrator_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tiendv89/workflow-orchestrator/internal/database/queries"
	"github.com/tiendv89/workflow-orchestrator/internal/github"
	"github.com/tiendv89/workflow-orchestrator/internal/orchestrator"
)

// mockPRGetter is a test double for github.PRGetter.
type mockPRGetter struct {
	// results maps prURL to the PRStatus to return (nil means return an error).
	results map[string]*github.PRStatus
	err     error // if non-nil, always return this error
}

func (m *mockPRGetter) GetPR(_ context.Context, prURL string) (*github.PRStatus, error) {
	if m.err != nil {
		return nil, m.err
	}
	if s, ok := m.results[prURL]; ok {
		return s, nil
	}
	// Default: PR not found — return an error so the test catches unexpected calls.
	return nil, errors.New("mock: unexpected prURL: " + prURL)
}

// insertInReviewTask creates a task in "in_review" status with an optional pr URL.
func insertInReviewTask(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	fx testFixture,
	taskName string,
	prURL string,
	dependsOn []string,
) uuid.UUID {
	t.Helper()
	q := queries.New(pool)
	taskID := uuid.New()
	status := "in_review"
	repo := "test-repo"
	owner := "go"

	depJSON, _ := json.Marshal(dependsOn)

	_, err := q.InsertTask(ctx, queries.InsertTaskParams{
		WorkspaceID: fx.workspaceID,
		FeatureID:   fx.featureID,
		FeatureName: "test-feature",
		TaskID:      taskID,
		TaskName:    taskName,
		Title:       "Test task " + taskName,
		Repo:        &repo,
		Status:      &status,
		DependsOn:   depJSON,
		Owner:       &owner,
	})
	if err != nil {
		t.Fatalf("insertInReviewTask %s: %v", taskName, err)
	}

	if prURL != "" {
		prJSON, _ := json.Marshal(map[string]string{"url": prURL, "status": "open"})
		_, err = pool.Exec(ctx,
			`UPDATE workspace_tasks SET pr = $1::jsonb WHERE workspace_id = $2 AND task_id = $3`,
			string(prJSON), fx.workspaceID, taskID,
		)
		if err != nil {
			t.Fatalf("set pr on task %s: %v", taskName, err)
		}
	}

	return taskID
}

// insertTodoTask creates a task in "todo" status with a depends_on list.
func insertTodoTask(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	fx testFixture,
	taskName string,
	dependsOn []string,
) uuid.UUID {
	t.Helper()
	q := queries.New(pool)
	taskID := uuid.New()
	status := "todo"
	repo := "test-repo"
	owner := "go"

	depJSON, _ := json.Marshal(dependsOn)

	_, err := q.InsertTask(ctx, queries.InsertTaskParams{
		WorkspaceID: fx.workspaceID,
		FeatureID:   fx.featureID,
		FeatureName: "test-feature",
		TaskID:      taskID,
		TaskName:    taskName,
		Title:       "Todo task " + taskName,
		Repo:        &repo,
		Status:      &status,
		DependsOn:   depJSON,
		Owner:       &owner,
	})
	if err != nil {
		t.Fatalf("insertTodoTask %s: %v", taskName, err)
	}
	return taskID
}

// TestPollMergedPRs_MergedPR verifies that a task in "in_review" with a merged PR
// is transitioned to "done" and its dependents are auto-readied.
func TestPollMergedPRs_MergedPR(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	const prURL = "https://github.com/owner/repo/pull/1"
	taskID := insertInReviewTask(t, ctx, pool, fx, "T1", prURL, []string{})

	// T2 depends on T1 (should be auto-readied when T1 is done).
	dep2ID := insertTodoTask(t, ctx, pool, fx, "T2", []string{"T1"})

	mock := &mockPRGetter{
		results: map[string]*github.PRStatus{
			prURL: {Merged: true, State: "closed"},
		},
	}

	if err := orchestrator.PollMergedPRs(ctx, mock, pool, fx.workspaceID); err != nil {
		t.Fatalf("PollMergedPRs: %v", err)
	}

	// T1 must be done.
	task1 := getTaskRow(t, ctx, pool, fx, taskID)
	if task1.Status == nil || *task1.Status != "done" {
		t.Errorf("T1 status = %v, want done", task1.Status)
	}

	// T2 must be ready (its only dep T1 is now done).
	task2 := getTaskRow(t, ctx, pool, fx, dep2ID)
	if task2.Status == nil || *task2.Status != "ready" {
		t.Errorf("T2 status = %v, want ready", task2.Status)
	}
}

// TestPollMergedPRs_OpenPR verifies that a task with an open (not merged) PR is
// left unchanged.
func TestPollMergedPRs_OpenPR(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	const prURL = "https://github.com/owner/repo/pull/2"
	taskID := insertInReviewTask(t, ctx, pool, fx, "T1", prURL, []string{})

	mock := &mockPRGetter{
		results: map[string]*github.PRStatus{
			prURL: {Merged: false, State: "open"},
		},
	}

	if err := orchestrator.PollMergedPRs(ctx, mock, pool, fx.workspaceID); err != nil {
		t.Fatalf("PollMergedPRs: %v", err)
	}

	// T1 must still be in_review.
	task := getTaskRow(t, ctx, pool, fx, taskID)
	if task.Status == nil || *task.Status != "in_review" {
		t.Errorf("T1 status = %v, want in_review (unchanged)", task.Status)
	}
}

// TestPollMergedPRs_GitHubAPIError verifies that a GitHub API error on one PR is
// logged and the poll loop continues without aborting.
func TestPollMergedPRs_GitHubAPIError(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	const prURL1 = "https://github.com/owner/repo/pull/10"
	const prURL2 = "https://github.com/owner/repo/pull/11"

	// T1: GitHub returns an error.
	task1ID := insertInReviewTask(t, ctx, pool, fx, "T1", prURL1, []string{})
	// T2: GitHub says merged — should still be processed.
	task2ID := insertInReviewTask(t, ctx, pool, fx, "T2", prURL2, []string{})

	targeted := &targetedMockPRGetter{
		failURL: prURL1,
		failErr: errors.New("GitHub API error (simulated)"),
		successURL: map[string]*github.PRStatus{
			prURL2: {Merged: true, State: "closed"},
		},
	}

	if err := orchestrator.PollMergedPRs(ctx, targeted, pool, fx.workspaceID); err != nil {
		t.Fatalf("PollMergedPRs should not return error on API failure: %v", err)
	}

	// T1 must remain in_review (GitHub errored, so no transition).
	row1 := getTaskRow(t, ctx, pool, fx, task1ID)
	if row1.Status == nil || *row1.Status != "in_review" {
		t.Errorf("T1 status = %v, want in_review (API error — must be skipped)", row1.Status)
	}

	// T2 must be done (merged PR, no error).
	row2 := getTaskRow(t, ctx, pool, fx, task2ID)
	if row2.Status == nil || *row2.Status != "done" {
		t.Errorf("T2 status = %v, want done (merged PR)", row2.Status)
	}
}

// targetedMockPRGetter returns an error for failURL and the success map otherwise.
type targetedMockPRGetter struct {
	failURL    string
	failErr    error
	successURL map[string]*github.PRStatus
}

func (m *targetedMockPRGetter) GetPR(_ context.Context, prURL string) (*github.PRStatus, error) {
	if prURL == m.failURL {
		return nil, m.failErr
	}
	if s, ok := m.successURL[prURL]; ok {
		return s, nil
	}
	return nil, errors.New("mock: unexpected prURL: " + prURL)
}

// TestPollMergedPRs_NoPRURL verifies that tasks without a PR URL are skipped silently.
func TestPollMergedPRs_NoPRURL(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	// Insert task with no PR URL.
	taskID := insertInReviewTask(t, ctx, pool, fx, "T1", "", []string{})

	// Mock should never be called — any call panics to detect unexpected interaction.
	mock := &mockPRGetter{err: errors.New("GetPR should not be called for tasks with no PR URL")}

	if err := orchestrator.PollMergedPRs(ctx, mock, pool, fx.workspaceID); err != nil {
		t.Fatalf("PollMergedPRs: %v", err)
	}

	// Task must remain in_review.
	task := getTaskRow(t, ctx, pool, fx, taskID)
	if task.Status == nil || *task.Status != "in_review" {
		t.Errorf("status = %v, want in_review (no PR URL — must be skipped)", task.Status)
	}
}
