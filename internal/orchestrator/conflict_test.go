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

// insertTaskWithConflictState inserts a task with an explicit conflict_state.
func insertTaskWithConflictState(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	fx testFixture,
	status, conflictState string,
	rebaseAttempts int32,
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
		t.Fatalf("insertTaskWithConflictState: insert: %v", err)
	}

	// Set conflict_state and rebase_attempts (not in the insert params, set via raw SQL).
	_, err = pool.Exec(ctx,
		`UPDATE workspace_tasks SET conflict_state = $1, rebase_attempts = $2
		 WHERE workspace_id = $3 AND task_id = $4`,
		conflictState, rebaseAttempts, fx.workspaceID, taskID,
	)
	if err != nil {
		t.Fatalf("insertTaskWithConflictState: update conflict state: %v", err)
	}
	return taskID
}

// getConflictState fetches the current conflict_state for a task.
func getConflictState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fx testFixture, taskID uuid.UUID) string {
	t.Helper()
	row := getTaskRow(t, ctx, pool, fx, taskID)
	return row.ConflictState
}

// getRebaseAttempts fetches the current rebase_attempts for a task.
func getRebaseAttempts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fx testFixture, taskID uuid.UUID) int32 {
	t.Helper()
	row := getTaskRow(t, ctx, pool, fx, taskID)
	return row.RebaseAttempts
}

// TestSetConflicted_FromNone verifies that SetConflicted transitions
// conflict_state from 'none' to 'conflicted'.
func TestSetConflicted_FromNone(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithConflictState(t, ctx, pool, fx, "in_review", "none", 0)

	ok, err := orchestrator.SetConflicted(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("SetConflicted: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	if cs := getConflictState(t, ctx, pool, fx, taskID); cs != "conflicted" {
		t.Errorf("conflict_state = %q, want conflicted", cs)
	}
}

// TestSetConflicted_FromResolved verifies that SetConflicted can re-conflicted
// after a successful rebase (resolved → conflicted).
func TestSetConflicted_FromResolved(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithConflictState(t, ctx, pool, fx, "in_review", "resolved", 0)

	ok, err := orchestrator.SetConflicted(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("SetConflicted: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	if cs := getConflictState(t, ctx, pool, fx, taskID); cs != "conflicted" {
		t.Errorf("conflict_state = %q, want conflicted", cs)
	}
}

// TestSetConflicted_DoesNotInterruptResolving verifies that SetConflicted
// is a no-op when an in-flight rebase is present (resolving state).
func TestSetConflicted_DoesNotInterruptResolving(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithConflictState(t, ctx, pool, fx, "in_review", "resolving", 1)

	ok, err := orchestrator.SetConflicted(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("SetConflicted: %v", err)
	}
	if ok {
		t.Error("expected ok=false (must not interrupt resolving)")
	}
	// conflict_state must remain 'resolving'.
	if cs := getConflictState(t, ctx, pool, fx, taskID); cs != "resolving" {
		t.Errorf("conflict_state = %q, want resolving (unchanged)", cs)
	}
}

// TestSetConflicted_SkipsTerminal verifies that SetConflicted does not
// affect tasks in terminal status (done/cancelled).
func TestSetConflicted_SkipsTerminal(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	for _, status := range []string{"done", "cancelled"} {
		taskID := insertTaskWithConflictState(t, ctx, pool, fx, status, "none", 0)
		ok, err := orchestrator.SetConflicted(ctx, pool, fx.workspaceID, taskID)
		if err != nil {
			t.Fatalf("SetConflicted (%s): %v", status, err)
		}
		if ok {
			t.Errorf("SetConflicted must be no-op for status=%s", status)
		}
	}
}

// TestSetResolving_Claim verifies the first-write-wins rebase dispatch claim.
func TestSetResolving_Claim(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithConflictState(t, ctx, pool, fx, "in_review", "conflicted", 0)

	handle := uuid.New().String()
	nonce := uuid.New().String()

	ok, err := orchestrator.SetResolving(ctx, pool, fx.workspaceID, taskID, handle, nonce)
	if err != nil {
		t.Fatalf("SetResolving: %v", err)
	}
	if !ok {
		t.Error("expected ok=true on first claim")
	}
	if cs := getConflictState(t, ctx, pool, fx, taskID); cs != "resolving" {
		t.Errorf("conflict_state = %q, want resolving", cs)
	}

	// Verify dispatch columns are set.
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.DispatchHandle == nil || *row.DispatchHandle != handle {
		t.Errorf("dispatch_handle = %v, want %q", row.DispatchHandle, handle)
	}
	if row.DispatchNonce == nil || *row.DispatchNonce != nonce {
		t.Errorf("dispatch_nonce = %v, want %q", row.DispatchNonce, nonce)
	}
	if row.DispatchKind == nil || *row.DispatchKind != "rebase" {
		t.Errorf("dispatch_kind = %v, want rebase", row.DispatchKind)
	}
}

// TestSetResolving_SecondClaimFails verifies that only the first claim wins.
func TestSetResolving_SecondClaimFails(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithConflictState(t, ctx, pool, fx, "in_review", "resolving", 0)

	// Already in resolving — second claim must fail.
	ok, err := orchestrator.SetResolving(ctx, pool, fx.workspaceID, taskID, uuid.New().String(), uuid.New().String())
	if err != nil {
		t.Fatalf("SetResolving: %v", err)
	}
	if ok {
		t.Error("expected ok=false when already resolving")
	}
}

// TestSetResolved_Success verifies that SetResolved clears resolving state and
// resets rebase_attempts to 0.
func TestSetResolved_Success(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithConflictState(t, ctx, pool, fx, "in_review", "resolving", 2)

	ok, err := orchestrator.SetResolved(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("SetResolved: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	if cs := getConflictState(t, ctx, pool, fx, taskID); cs != "resolved" {
		t.Errorf("conflict_state = %q, want resolved", cs)
	}
	if ra := getRebaseAttempts(t, ctx, pool, fx, taskID); ra != 0 {
		t.Errorf("rebase_attempts = %d, want 0 (reset on success)", ra)
	}
}

// TestSetResolved_WrongPrecondition verifies that SetResolved is a no-op when
// the task is not in 'resolving' state.
func TestSetResolved_WrongPrecondition(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithConflictState(t, ctx, pool, fx, "in_review", "conflicted", 0)

	ok, err := orchestrator.SetResolved(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("SetResolved: %v", err)
	}
	if ok {
		t.Error("expected ok=false when not in resolving state")
	}
}

// TestMarkRebaseRetry verifies that retry transitions resolving→conflicted and
// increments rebase_attempts.
func TestMarkRebaseRetry(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithConflictState(t, ctx, pool, fx, "in_review", "resolving", 1)

	ok, err := orchestrator.MarkRebaseRetry(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("MarkRebaseRetry: %v", err)
	}
	if !ok {
		t.Error("expected ok=true")
	}
	if cs := getConflictState(t, ctx, pool, fx, taskID); cs != "conflicted" {
		t.Errorf("conflict_state = %q, want conflicted", cs)
	}
	if ra := getRebaseAttempts(t, ctx, pool, fx, taskID); ra != 2 {
		t.Errorf("rebase_attempts = %d, want 2 (incremented)", ra)
	}
}

// TestMarkRebaseRetry_WrongPrecondition verifies that retry is a no-op when not
// in 'resolving' state.
func TestMarkRebaseRetry_WrongPrecondition(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithConflictState(t, ctx, pool, fx, "in_review", "conflicted", 0)

	ok, err := orchestrator.MarkRebaseRetry(ctx, pool, fx.workspaceID, taskID)
	if err != nil {
		t.Fatalf("MarkRebaseRetry: %v", err)
	}
	if ok {
		t.Error("expected ok=false when not in resolving state")
	}
}

// TestFindConflictedTasks verifies that only tasks with conflict_state='conflicted'
// and non-terminal status are returned.
func TestFindConflictedTasks(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	// Insert a conflicted task.
	conflictedID := insertTaskWithConflictState(t, ctx, pool, fx, "in_review", "conflicted", 0)
	// Insert a resolving task — must not be returned.
	_ = insertTaskWithConflictState(t, ctx, pool, fx, "in_review", "resolving", 0)
	// Insert a done task with conflicted state — must not be returned.
	_ = insertTaskWithConflictState(t, ctx, pool, fx, "done", "conflicted", 0)
	// Insert a task with no conflict — must not be returned.
	_ = insertTaskWithConflictState(t, ctx, pool, fx, "in_review", "none", 0)

	tasks, err := orchestrator.FindConflictedTasks(ctx, pool, fx.workspaceID)
	if err != nil {
		t.Fatalf("FindConflictedTasks: %v", err)
	}

	if len(tasks) != 1 {
		t.Fatalf("expected 1 conflicted task, got %d", len(tasks))
	}
	if tasks[0].TaskID != conflictedID {
		t.Errorf("wrong task returned: %v, want %v", tasks[0].TaskID, conflictedID)
	}
}

// TestListPRPollTasks verifies that ListPRPollTasks returns tasks in
// in_review, reviewing, and review_passed with a PR URL, but not tasks
// in other statuses or without a PR URL.
// TestListMergeablePRTasksForOwner exercises the PR-poll listing query used by
// PollMergedPRs. ListPRPollTasks (T8's original, hand-written version of this
// query) was a duplicate of the already-existing sqlc query — removed in favor
// of the canonical one; this test was repointed rather than deleted since it
// was the only integration coverage for this query shape.
func TestListMergeablePRTasksForOwner(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	insertWithPR := func(status, prURL string) uuid.UUID {
		taskID := insertTaskWithStatus(t, ctx, pool, fx, status)
		if prURL != "" {
			prJSON, _ := json.Marshal(map[string]string{"url": prURL, "status": "open"})
			_, err := pool.Exec(ctx,
				`UPDATE workspace_tasks SET pr = $1::jsonb WHERE workspace_id = $2 AND task_id = $3`,
				string(prJSON), fx.workspaceID, taskID,
			)
			if err != nil {
				t.Fatalf("set pr on task: %v", err)
			}
		}
		return taskID
	}

	// Tasks that should be returned.
	inReviewID := insertWithPR("in_review", "https://github.com/o/r/pull/1")
	reviewingID := insertWithPR("reviewing", "https://github.com/o/r/pull/2")
	reviewPassedID := insertWithPR("review_passed", "https://github.com/o/r/pull/3")

	// Tasks that should NOT be returned.
	_ = insertWithPR("in_review", "")                                // no PR URL
	_ = insertWithPR("in_progress", "https://github.com/o/r/pull/4") // wrong status
	_ = insertWithPR("done", "https://github.com/o/r/pull/5")        // terminal

	owner := "go"
	q := queries.New(pool)
	tasks, err := q.ListMergeablePRTasksForOwner(ctx, queries.ListMergeablePRTasksForOwnerParams{
		WorkspaceID: fx.workspaceID,
		Owner:       &owner,
	})
	if err != nil {
		t.Fatalf("ListMergeablePRTasksForOwner: %v", err)
	}

	got := make(map[uuid.UUID]bool)
	for _, task := range tasks {
		got[task.TaskID] = true
	}

	for _, id := range []uuid.UUID{inReviewID, reviewingID, reviewPassedID} {
		if !got[id] {
			t.Errorf("expected task %v in result, but not found", id)
		}
	}
	if len(tasks) != 3 {
		t.Errorf("got %d tasks, want exactly 3", len(tasks))
	}
}

// ---- Extended PR merge poll tests ----

// mockMergeableGetter extends mockPRGetter with mergeable field support.
type mockMergeableGetter struct {
	results map[string]*github.PRStatus
	err     error
}

func (m *mockMergeableGetter) GetPR(_ context.Context, prURL string) (*github.PRStatus, error) {
	if m.err != nil {
		return nil, m.err
	}
	if s, ok := m.results[prURL]; ok {
		return s, nil
	}
	return nil, errors.New("mock: unexpected prURL: " + prURL)
}

// insertReviewingTask creates a task in 'reviewing' status with a PR URL.
func insertReviewingTask(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	fx testFixture,
	taskName, prURL string,
) uuid.UUID {
	t.Helper()
	q := queries.New(pool)
	taskID := uuid.New()
	status := "reviewing"
	repo := "test-repo"
	owner := "go"

	_, err := q.InsertTask(ctx, queries.InsertTaskParams{
		WorkspaceID: fx.workspaceID,
		FeatureID:   fx.featureID,
		FeatureName: "test-feature",
		TaskID:      taskID,
		TaskName:    taskName,
		Title:       "Reviewing task " + taskName,
		Repo:        &repo,
		Status:      &status,
		DependsOn:   []byte("[]"),
		Owner:       &owner,
	})
	if err != nil {
		t.Fatalf("insertReviewingTask %s: %v", taskName, err)
	}
	if prURL != "" {
		prJSON, _ := json.Marshal(map[string]string{"url": prURL, "status": "open"})
		_, err = pool.Exec(ctx,
			`UPDATE workspace_tasks SET pr = $1::jsonb WHERE workspace_id = $2 AND task_id = $3`,
			string(prJSON), fx.workspaceID, taskID,
		)
		if err != nil {
			t.Fatalf("set pr on reviewing task %s: %v", taskName, err)
		}
	}
	return taskID
}

// insertReviewPassedTask creates a task in 'review_passed' status with a PR URL.
func insertReviewPassedTask(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	fx testFixture,
	taskName, prURL string,
) uuid.UUID {
	t.Helper()
	q := queries.New(pool)
	taskID := uuid.New()
	status := "review_passed"
	repo := "test-repo"
	owner := "go"

	_, err := q.InsertTask(ctx, queries.InsertTaskParams{
		WorkspaceID: fx.workspaceID,
		FeatureID:   fx.featureID,
		FeatureName: "test-feature",
		TaskID:      taskID,
		TaskName:    taskName,
		Title:       "Review-passed task " + taskName,
		Repo:        &repo,
		Status:      &status,
		DependsOn:   []byte("[]"),
		Owner:       &owner,
	})
	if err != nil {
		t.Fatalf("insertReviewPassedTask %s: %v", taskName, err)
	}
	if prURL != "" {
		prJSON, _ := json.Marshal(map[string]string{"url": prURL, "status": "open"})
		_, err = pool.Exec(ctx,
			`UPDATE workspace_tasks SET pr = $1::jsonb WHERE workspace_id = $2 AND task_id = $3`,
			string(prJSON), fx.workspaceID, taskID,
		)
		if err != nil {
			t.Fatalf("set pr on review_passed task %s: %v", taskName, err)
		}
	}
	return taskID
}

// TestPollMergedPRs_MergedFromReviewing verifies that a task in 'reviewing'
// with a merged PR is transitioned to 'done' (merged-is-ground-truth).
func TestPollMergedPRs_MergedFromReviewing(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	const prURL = "https://github.com/owner/repo/pull/20"
	taskID := insertReviewingTask(t, ctx, pool, fx, "T1", prURL)

	mock := &mockMergeableGetter{
		results: map[string]*github.PRStatus{
			prURL: {Merged: true, State: "closed"},
		},
	}

	if err := orchestrator.PollMergedPRs(ctx, mock, pool, fx.workspaceID); err != nil {
		t.Fatalf("PollMergedPRs: %v", err)
	}

	task := getTaskRow(t, ctx, pool, fx, taskID)
	if task.Status == nil || *task.Status != "done" {
		t.Errorf("status = %v, want done (merged-is-ground-truth from reviewing)", task.Status)
	}
}

// TestPollMergedPRs_MergedFromReviewPassed verifies that a task in 'review_passed'
// with a merged PR is transitioned to 'done'.
func TestPollMergedPRs_MergedFromReviewPassed(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	const prURL = "https://github.com/owner/repo/pull/21"
	taskID := insertReviewPassedTask(t, ctx, pool, fx, "T1", prURL)

	mock := &mockMergeableGetter{
		results: map[string]*github.PRStatus{
			prURL: {Merged: true, State: "closed"},
		},
	}

	if err := orchestrator.PollMergedPRs(ctx, mock, pool, fx.workspaceID); err != nil {
		t.Fatalf("PollMergedPRs: %v", err)
	}

	task := getTaskRow(t, ctx, pool, fx, taskID)
	if task.Status == nil || *task.Status != "done" {
		t.Errorf("status = %v, want done (merged-is-ground-truth from review_passed)", task.Status)
	}
}

// TestPollMergedPRs_ConflictDetected verifies that a CONFLICTING PR
// sets conflict_state='conflicted'.
func TestPollMergedPRs_ConflictDetected(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	const prURL = "https://github.com/owner/repo/pull/30"
	taskID := insertInReviewTask(t, ctx, pool, fx, "T1", prURL, []string{})

	mock := &mockMergeableGetter{
		results: map[string]*github.PRStatus{
			prURL: {Merged: false, State: "open", Mergeable: "CONFLICTING"},
		},
	}

	if err := orchestrator.PollMergedPRs(ctx, mock, pool, fx.workspaceID); err != nil {
		t.Fatalf("PollMergedPRs: %v", err)
	}

	task := getTaskRow(t, ctx, pool, fx, taskID)
	// Status must still be in_review — only conflict_state changes.
	if task.Status == nil || *task.Status != "in_review" {
		t.Errorf("status = %v, want in_review (unchanged)", task.Status)
	}
	if task.ConflictState != "conflicted" {
		t.Errorf("conflict_state = %q, want conflicted", task.ConflictState)
	}
}

// TestPollMergedPRs_ConflictUnknownSkipped verifies that UNKNOWN mergeable
// state causes no conflict_state change (recheck next cycle).
func TestPollMergedPRs_ConflictUnknownSkipped(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	const prURL = "https://github.com/owner/repo/pull/31"
	taskID := insertInReviewTask(t, ctx, pool, fx, "T1", prURL, []string{})

	mock := &mockMergeableGetter{
		results: map[string]*github.PRStatus{
			prURL: {Merged: false, State: "open", Mergeable: "UNKNOWN"},
		},
	}

	if err := orchestrator.PollMergedPRs(ctx, mock, pool, fx.workspaceID); err != nil {
		t.Fatalf("PollMergedPRs: %v", err)
	}

	task := getTaskRow(t, ctx, pool, fx, taskID)
	if task.ConflictState != "none" {
		t.Errorf("conflict_state = %q, want none (UNKNOWN skipped)", task.ConflictState)
	}
}

// TestPollMergedPRs_ConflictDoesNotInterruptResolving verifies that when a task
// is already in 'resolving', a CONFLICTING poll result does not clobber the state.
func TestPollMergedPRs_ConflictDoesNotInterruptResolving(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	const prURL = "https://github.com/owner/repo/pull/32"
	taskID := insertInReviewTask(t, ctx, pool, fx, "T1", prURL, []string{})

	// Pre-set conflict_state to 'resolving'.
	_, err := pool.Exec(ctx,
		`UPDATE workspace_tasks SET conflict_state = 'resolving' WHERE workspace_id = $1 AND task_id = $2`,
		fx.workspaceID, taskID,
	)
	if err != nil {
		t.Fatalf("set conflict_state: %v", err)
	}

	mock := &mockMergeableGetter{
		results: map[string]*github.PRStatus{
			prURL: {Merged: false, State: "open", Mergeable: "CONFLICTING"},
		},
	}

	if err := orchestrator.PollMergedPRs(ctx, mock, pool, fx.workspaceID); err != nil {
		t.Fatalf("PollMergedPRs: %v", err)
	}

	task := getTaskRow(t, ctx, pool, fx, taskID)
	if task.ConflictState != "resolving" {
		t.Errorf("conflict_state = %q, want resolving (not interrupted)", task.ConflictState)
	}
}

// ---- HandleRebaseCompletion tests (integration) ----

// TestHandleRebaseCompletion_Success verifies that a successful rebase
// transitions conflict_state to 'resolved' and resets rebase_attempts.
func TestHandleRebaseCompletion_Success(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithConflictState(t, ctx, pool, fx, "in_review", "resolving", 1)

	if err := orchestrator.HandleRebaseCompletion(ctx, pool, fx.workspaceID, taskID, true, 3); err != nil {
		t.Fatalf("HandleRebaseCompletion: %v", err)
	}

	task := getTaskRow(t, ctx, pool, fx, taskID)
	if task.ConflictState != "resolved" {
		t.Errorf("conflict_state = %q, want resolved", task.ConflictState)
	}
	if task.RebaseAttempts != 0 {
		t.Errorf("rebase_attempts = %d, want 0 (reset on success)", task.RebaseAttempts)
	}
}

// TestHandleRebaseCompletion_FailureBelowCap verifies that a failed rebase
// below the cap retries (resolving → conflicted, increments counter).
func TestHandleRebaseCompletion_FailureBelowCap(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	// rebase_attempts=1, cap=3 → nextAttempts=2 < 3 → retry
	taskID := insertTaskWithConflictState(t, ctx, pool, fx, "in_review", "resolving", 1)

	if err := orchestrator.HandleRebaseCompletion(ctx, pool, fx.workspaceID, taskID, false, 3); err != nil {
		t.Fatalf("HandleRebaseCompletion: %v", err)
	}

	task := getTaskRow(t, ctx, pool, fx, taskID)
	if task.ConflictState != "conflicted" {
		t.Errorf("conflict_state = %q, want conflicted (retry)", task.ConflictState)
	}
	if task.RebaseAttempts != 2 {
		t.Errorf("rebase_attempts = %d, want 2 (incremented)", task.RebaseAttempts)
	}
	if task.Status == nil || *task.Status != "in_review" {
		t.Errorf("status = %v, want in_review (unchanged on retry)", task.Status)
	}
}

// TestHandleRebaseCompletion_PathA_CapReached verifies that an in_review task
// is blocked when the rebase cap is reached (Path A).
func TestHandleRebaseCompletion_PathA_CapReached(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	// rebase_attempts=2, cap=3 → nextAttempts=3 >= 3 → block (Path A: in_review)
	taskID := insertTaskWithConflictState(t, ctx, pool, fx, "in_review", "resolving", 2)

	if err := orchestrator.HandleRebaseCompletion(ctx, pool, fx.workspaceID, taskID, false, 3); err != nil {
		t.Fatalf("HandleRebaseCompletion: %v", err)
	}

	task := getTaskRow(t, ctx, pool, fx, taskID)
	if task.Status == nil || *task.Status != "blocked" {
		t.Errorf("status = %v, want blocked (Path A cap reached)", task.Status)
	}
	if task.BlockedReason == nil || *task.BlockedReason != "rebase_failed" {
		t.Errorf("blocked_reason = %v, want rebase_failed", task.BlockedReason)
	}
}

// TestHandleRebaseCompletion_PathB_CapReached verifies that a review_passed task
// stays in review_passed + conflicted (not blocked) when the cap is reached (Path B).
func TestHandleRebaseCompletion_PathB_CapReached(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	// rebase_attempts=2, cap=3 → nextAttempts=3 >= 3 → stay conflicted (Path B: review_passed)
	taskID := insertTaskWithConflictState(t, ctx, pool, fx, "review_passed", "resolving", 2)

	if err := orchestrator.HandleRebaseCompletion(ctx, pool, fx.workspaceID, taskID, false, 3); err != nil {
		t.Fatalf("HandleRebaseCompletion: %v", err)
	}

	task := getTaskRow(t, ctx, pool, fx, taskID)
	// Must NOT be blocked — Path B keeps task in review_passed.
	if task.Status == nil || *task.Status != "review_passed" {
		t.Errorf("status = %v, want review_passed (Path B — must not block)", task.Status)
	}
	// conflict_state should be 'conflicted' (cleared from resolving, not blocked).
	if task.ConflictState != "conflicted" {
		t.Errorf("conflict_state = %q, want conflicted (Path B stays conflicted)", task.ConflictState)
	}
}
