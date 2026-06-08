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

// TestAutoReadyDependents_DiamondDependency verifies the diamond dependency case:
// T1→{T2,T3}→T4. Marking T1 done readies T2 and T3 but not T4.
// When T2 and T3 are marked done, T4 becomes ready.
func TestAutoReadyDependents_DiamondDependency(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	// Seed tasks: T1 (in_review), T2 (todo, dep T1), T3 (todo, dep T1), T4 (todo, dep T2+T3).
	t1ID := insertInReviewTask(t, ctx, pool, fx, "T1", "https://github.com/o/r/pull/1", []string{})
	t2ID := insertTodoTask(t, ctx, pool, fx, "T2", []string{"T1"})
	t3ID := insertTodoTask(t, ctx, pool, fx, "T3", []string{"T1"})
	t4ID := insertTodoTask(t, ctx, pool, fx, "T4", []string{"T2", "T3"})

	// Mark T1 merged — T2 and T3 should become ready; T4 stays todo.
	mock := &mockPRGetter{
		results: map[string]*github.PRStatus{
			"https://github.com/o/r/pull/1": {Merged: true, State: "closed"},
		},
	}
	if err := orchestrator.PollMergedPRs(ctx, mock, pool, fx.workspaceID); err != nil {
		t.Fatalf("PollMergedPRs (mark T1 done): %v", err)
	}

	assertStatus(t, ctx, pool, fx, t1ID, "done")
	assertStatus(t, ctx, pool, fx, t2ID, "ready")
	assertStatus(t, ctx, pool, fx, t3ID, "ready")
	assertStatus(t, ctx, pool, fx, t4ID, "todo") // T2 and T3 not yet done

	// Mark T2 done (via direct SetInReview trick then PollMergedPRs).
	// Simpler: mark T2 done via direct update and call AutoReadyDependents via a
	// standalone transaction to validate the function's behaviour.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx: %v", err)
	}
	_, err = tx.Exec(ctx,
		`UPDATE workspace_tasks SET status='done', updated_at=now() WHERE workspace_id=$1 AND task_id=$2`,
		fx.workspaceID, t2ID,
	)
	if err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		t.Fatalf("mark T2 done: %v", err)
	}
	advanced, err := orchestrator.AutoReadyDependents(ctx, tx, fx.workspaceID, "T2")
	if err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		t.Fatalf("AutoReadyDependents after T2: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// T4 still has unsatisfied dep T3 — must not be in advanced list.
	for _, id := range advanced {
		if id == t4ID {
			t.Error("T4 should not be auto-readied when T3 is still todo")
		}
	}

	// Now mark T3 done and call AutoReadyDependents — T4 should become ready.
	tx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx2: %v", err)
	}
	_, err = tx2.Exec(ctx,
		`UPDATE workspace_tasks SET status='done', updated_at=now() WHERE workspace_id=$1 AND task_id=$2`,
		fx.workspaceID, t3ID,
	)
	if err != nil {
		tx2.Rollback(ctx) //nolint:errcheck
		t.Fatalf("mark T3 done: %v", err)
	}
	advanced2, err := orchestrator.AutoReadyDependents(ctx, tx2, fx.workspaceID, "T3")
	if err != nil {
		tx2.Rollback(ctx) //nolint:errcheck
		t.Fatalf("AutoReadyDependents after T3: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("commit tx2: %v", err)
	}

	// T4 must now be ready.
	found := false
	for _, id := range advanced2 {
		if id == t4ID {
			found = true
		}
	}
	if !found {
		t.Errorf("T4 was not auto-readied after T2 and T3 are both done; advanced=%v", advanced2)
	}
	assertStatus(t, ctx, pool, fx, t4ID, "ready")
}

// TestAutoReadyDependents_NoQualifyingTask verifies that no-op returns empty slice.
func TestAutoReadyDependents_NoQualifyingTask(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	advanced, err := orchestrator.AutoReadyDependents(ctx, tx, fx.workspaceID, "nonexistent-task")
	if err != nil {
		t.Fatalf("AutoReadyDependents: %v", err)
	}
	if len(advanced) != 0 {
		t.Errorf("expected empty slice, got %v", advanced)
	}
}

// assertStatus is a helper that fetches a task row and checks its status field.
func assertStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fx testFixture, taskID uuid.UUID, want string) {
	t.Helper()
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil || *row.Status != want {
		t.Errorf("task %s status = %v, want %q", taskID, row.Status, want)
	}
}
