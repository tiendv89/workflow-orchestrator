package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tiendv89/workflow-orchestrator/internal/config"
	db "github.com/tiendv89/workflow-orchestrator/internal/database/queries"
)

// --- helpers ---

func ptrStr(s string) *string { return &s }

func makeDispatchedTask(featureName, taskName, handle, nonce, kind string, attempts int32, status string) db.WorkspaceTask {
	dispatched := pgtype.Timestamptz{
		Time:  time.Now().UTC().Add(-3 * time.Hour), // older than 2h deadline
		Valid: true,
	}
	return db.WorkspaceTask{
		TaskID:            uuid.New(),
		FeatureID:         uuid.New(),
		FeatureName:       featureName,
		TaskName:          taskName,
		Status:            &status,
		DispatchHandle:    ptrStr(handle),
		DispatchNonce:     ptrStr(nonce),
		DispatchKind:      ptrStr(kind),
		ReenqueueAttempts: attempts,
		DispatchedAt:      dispatched,
		Repo:              ptrStr("impl-repo"),
		Branch:            ptrStr("feature/" + featureName + "-" + taskName),
	}
}

func makeRecentTask(featureName, taskName string) db.WorkspaceTask {
	dispatched := pgtype.Timestamptz{
		Time:  time.Now().UTC().Add(-30 * time.Minute), // within 2h deadline
		Valid: true,
	}
	status := "in_progress"
	return db.WorkspaceTask{
		TaskID:         uuid.New(),
		FeatureID:      uuid.New(),
		FeatureName:    featureName,
		TaskName:       taskName,
		Status:         &status,
		DispatchHandle: ptrStr("handle-recent"),
		DispatchNonce:  ptrStr("nonce-recent"),
		DispatchedAt:   dispatched,
	}
}

// fakeReconciler captures calls to bumpAttempts, setBlocked, and enqueue.
type fakeReconcilerDeps struct {
	bumpCalls    []uuid.UUID
	bumpReturn   int32
	bumpErr      error
	blockedCalls []struct {
		taskUUID   uuid.UUID
		reason     string
		details    string
		fromStatus string
	}
	blockedErr   error
	enqueueCalls []struct {
		handle string
		nonce  string
		kind   string
	}
	enqueueErr error
}

func (f *fakeReconcilerDeps) bump(_ context.Context, _ *pgxpool.Pool, _, taskUUID uuid.UUID) (int32, error) {
	f.bumpCalls = append(f.bumpCalls, taskUUID)
	return f.bumpReturn, f.bumpErr
}

func (f *fakeReconcilerDeps) blocked(_ context.Context, _ *pgxpool.Pool, _, taskUUID uuid.UUID, reason, details, fromStatus string) (bool, error) {
	f.blockedCalls = append(f.blockedCalls, struct {
		taskUUID   uuid.UUID
		reason     string
		details    string
		fromStatus string
	}{taskUUID, reason, details, fromStatus})
	return true, f.blockedErr
}

func (f *fakeReconcilerDeps) enqueue(_ context.Context, _ *config.Config, _ db.WorkspaceTask, handle, nonce, kind string) error {
	f.enqueueCalls = append(f.enqueueCalls, struct {
		handle string
		nonce  string
		kind   string
	}{handle, nonce, kind})
	return f.enqueueErr
}

func newTestReconciler(deps *fakeReconcilerDeps, tasks []db.WorkspaceTask, maxRetries, deadlineMS int) *Reconciler {
	return &Reconciler{
		findStuck: func(_ context.Context, _ *db.Queries, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return tasks, nil
		},
		bumpAttempts: deps.bump,
		setBlocked:   deps.blocked,
		enqueue:      deps.enqueue,
		maxRetries:   maxRetries,
		deadlineMS:   deadlineMS,
	}
}

// --- tests ---

// TestReconciler_ReenqueuesStuckTask verifies the happy path: a stuck task
// (dispatched_at > deadline, attempts < max) gets its counter bumped and is re-enqueued.
func TestReconciler_ReenqueuesStuckTask(t *testing.T) {
	wsID := uuid.New()
	task := makeDispatchedTask("feat", "T1", "h1", "n1", "impl", 0, "in_progress")

	deps := &fakeReconcilerDeps{bumpReturn: 1}
	r := newTestReconciler(deps, []db.WorkspaceTask{task}, 3, 7200000)

	if err := r.reconcile(context.Background(), nil, nil, wsID); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	if len(deps.bumpCalls) != 1 {
		t.Fatalf("bumpAttempts called %d times, want 1", len(deps.bumpCalls))
	}
	if deps.bumpCalls[0] != task.TaskID {
		t.Errorf("bumpAttempts taskUUID = %v, want %v", deps.bumpCalls[0], task.TaskID)
	}
	if len(deps.enqueueCalls) != 1 {
		t.Fatalf("enqueue called %d times, want 1", len(deps.enqueueCalls))
	}
	if deps.enqueueCalls[0].handle != "h1" {
		t.Errorf("enqueue handle = %q, want h1", deps.enqueueCalls[0].handle)
	}
	if deps.enqueueCalls[0].nonce != "n1" {
		t.Errorf("enqueue nonce = %q, want n1", deps.enqueueCalls[0].nonce)
	}
	if deps.enqueueCalls[0].kind != "impl" {
		t.Errorf("enqueue kind = %q, want impl", deps.enqueueCalls[0].kind)
	}
	if len(deps.blockedCalls) != 0 {
		t.Errorf("setBlocked should not be called; called %d times", len(deps.blockedCalls))
	}
}

// TestReconciler_BumpsBeforeEnqueue verifies that the attempt counter is
// incremented before enqueue is called (durable-before-enqueue guarantee).
func TestReconciler_BumpsBeforeEnqueue(t *testing.T) {
	wsID := uuid.New()
	task := makeDispatchedTask("feat", "T2", "h2", "n2", "fix", 1, "in_progress")

	var callOrder []string
	deps := &fakeReconcilerDeps{}
	r := newTestReconciler(deps, []db.WorkspaceTask{task}, 3, 7200000)

	// Override to track call order.
	r.bumpAttempts = func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID) (int32, error) {
		callOrder = append(callOrder, "bump")
		return 2, nil
	}
	r.enqueue = func(_ context.Context, _ *config.Config, _ db.WorkspaceTask, _, _, _ string) error {
		callOrder = append(callOrder, "enqueue")
		return nil
	}

	if err := r.reconcile(context.Background(), nil, nil, wsID); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	if len(callOrder) != 2 || callOrder[0] != "bump" || callOrder[1] != "enqueue" {
		t.Errorf("call order = %v, want [bump enqueue]", callOrder)
	}
}

// TestReconciler_BlocksAtMaxRetries verifies that a task at or above maxRetries
// is blocked with "reconciler_max" and not re-enqueued.
func TestReconciler_BlocksAtMaxRetries(t *testing.T) {
	wsID := uuid.New()
	task := makeDispatchedTask("feat", "T3", "h3", "n3", "review", 3, "reviewing") // attempts == maxRetries

	deps := &fakeReconcilerDeps{}
	r := newTestReconciler(deps, []db.WorkspaceTask{task}, 3, 7200000)

	if err := r.reconcile(context.Background(), nil, nil, wsID); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	if len(deps.blockedCalls) != 1 {
		t.Fatalf("setBlocked called %d times, want 1", len(deps.blockedCalls))
	}
	if deps.blockedCalls[0].reason != "reconciler_max" {
		t.Errorf("reason = %q, want reconciler_max", deps.blockedCalls[0].reason)
	}
	if deps.blockedCalls[0].fromStatus != "reviewing" {
		t.Errorf("fromStatus = %q, want reviewing", deps.blockedCalls[0].fromStatus)
	}
	if len(deps.bumpCalls) != 0 {
		t.Errorf("bumpAttempts should not be called when blocking; called %d times", len(deps.bumpCalls))
	}
	if len(deps.enqueueCalls) != 0 {
		t.Errorf("enqueue should not be called when blocking; called %d times", len(deps.enqueueCalls))
	}
}

// TestReconciler_AboveMaxRetries verifies tasks with attempts > maxRetries are also blocked.
func TestReconciler_AboveMaxRetries(t *testing.T) {
	wsID := uuid.New()
	task := makeDispatchedTask("feat", "T4", "h4", "n4", "impl", 5, "in_progress") // attempts > maxRetries

	deps := &fakeReconcilerDeps{}
	r := newTestReconciler(deps, []db.WorkspaceTask{task}, 3, 7200000)

	if err := r.reconcile(context.Background(), nil, nil, wsID); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	if len(deps.blockedCalls) != 1 {
		t.Errorf("setBlocked called %d times, want 1", len(deps.blockedCalls))
	}
}

// TestReconciler_SkipsRecentTask verifies that tasks dispatched within the
// deadline window are not processed (no bump, no enqueue, no block).
func TestReconciler_SkipsRecentTask(t *testing.T) {
	wsID := uuid.New()
	task := makeRecentTask("feat", "T5")

	deps := &fakeReconcilerDeps{}
	r := newTestReconciler(deps, []db.WorkspaceTask{task}, 3, 7200000)

	if err := r.reconcile(context.Background(), nil, nil, wsID); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	if len(deps.bumpCalls) != 0 || len(deps.enqueueCalls) != 0 || len(deps.blockedCalls) != 0 {
		t.Error("no operations should be applied to a task within the deadline window")
	}
}

// TestReconciler_SkipsTaskWithoutHandle verifies that tasks with a nil
// dispatch_handle are skipped (not yet dispatched).
func TestReconciler_SkipsTaskWithoutHandle(t *testing.T) {
	wsID := uuid.New()
	status := "in_progress"
	task := db.WorkspaceTask{
		TaskID:         uuid.New(),
		FeatureName:    "feat",
		TaskName:       "T6",
		Status:         &status,
		DispatchHandle: nil,
		DispatchNonce:  nil,
	}

	deps := &fakeReconcilerDeps{}
	r := newTestReconciler(deps, []db.WorkspaceTask{task}, 3, 7200000)

	if err := r.reconcile(context.Background(), nil, nil, wsID); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	if len(deps.bumpCalls) != 0 || len(deps.enqueueCalls) != 0 || len(deps.blockedCalls) != 0 {
		t.Error("no operations should be applied to a task without a dispatch handle")
	}
}

// TestReconciler_RedisFailureDoesNotBlockTask verifies that a Redis enqueue
// failure does not block the task (infra-level, no task state change).
func TestReconciler_RedisFailureDoesNotBlockTask(t *testing.T) {
	wsID := uuid.New()
	task := makeDispatchedTask("feat", "T7", "h7", "n7", "impl", 0, "in_progress")

	deps := &fakeReconcilerDeps{
		bumpReturn: 1,
		enqueueErr: errors.New("redis: connection refused"),
	}
	r := newTestReconciler(deps, []db.WorkspaceTask{task}, 3, 7200000)

	if err := r.reconcile(context.Background(), nil, nil, wsID); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	// Bump must still happen.
	if len(deps.bumpCalls) != 1 {
		t.Fatalf("bumpAttempts should be called even when Redis fails; got %d", len(deps.bumpCalls))
	}
	// Task must NOT be blocked — Redis failure is infra-level.
	if len(deps.blockedCalls) != 0 {
		t.Errorf("setBlocked must not be called on Redis failure; called %d times", len(deps.blockedCalls))
	}
}

// TestReconciler_DefaultKind verifies that a nil dispatch_kind defaults to "impl".
func TestReconciler_DefaultKind(t *testing.T) {
	wsID := uuid.New()
	dispatched := pgtype.Timestamptz{Time: time.Now().UTC().Add(-3 * time.Hour), Valid: true}
	status := "in_progress"
	task := db.WorkspaceTask{
		TaskID:            uuid.New(),
		FeatureName:       "feat",
		TaskName:          "T8",
		Status:            &status,
		DispatchHandle:    ptrStr("h8"),
		DispatchNonce:     ptrStr("n8"),
		DispatchKind:      nil, // nil — should default to "impl"
		ReenqueueAttempts: 0,
		DispatchedAt:      dispatched,
	}

	deps := &fakeReconcilerDeps{bumpReturn: 1}
	r := newTestReconciler(deps, []db.WorkspaceTask{task}, 3, 7200000)

	if err := r.reconcile(context.Background(), nil, nil, wsID); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	if len(deps.enqueueCalls) != 1 {
		t.Fatalf("enqueue called %d times, want 1", len(deps.enqueueCalls))
	}
	if deps.enqueueCalls[0].kind != "impl" {
		t.Errorf("kind = %q, want impl (default)", deps.enqueueCalls[0].kind)
	}
}

// TestReconciler_MultipleTasksPartialBlock verifies that blocking one task does
// not prevent processing of subsequent tasks.
func TestReconciler_MultipleTasksPartialBlock(t *testing.T) {
	wsID := uuid.New()
	// Task 1: at max retries → block.
	task1 := makeDispatchedTask("feat", "T1", "h1", "n1", "impl", 3, "in_progress")
	// Task 2: below max retries → re-enqueue.
	task2 := makeDispatchedTask("feat", "T2", "h2", "n2", "impl", 1, "in_progress")

	deps := &fakeReconcilerDeps{bumpReturn: 2}
	r := newTestReconciler(deps, []db.WorkspaceTask{task1, task2}, 3, 7200000)

	if err := r.reconcile(context.Background(), nil, nil, wsID); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	if len(deps.blockedCalls) != 1 {
		t.Errorf("setBlocked called %d times, want 1", len(deps.blockedCalls))
	}
	if len(deps.enqueueCalls) != 1 {
		t.Errorf("enqueue called %d times, want 1 (for task2)", len(deps.enqueueCalls))
	}
	if deps.enqueueCalls[0].handle != "h2" {
		t.Errorf("enqueued handle = %q, want h2", deps.enqueueCalls[0].handle)
	}
}

// TestReconciler_SkipsMergedReviewingTask verifies that a stuck "reviewing"
// task whose PR is already merged is not re-enqueued or blocked. PollMergedPRs
// (later in the cycle) will mark it done; re-enqueuing would dispatch a
// redundant reviewer.
func TestReconciler_SkipsMergedReviewingTask(t *testing.T) {
	wsID := uuid.New()
	prURL := "https://github.com/o/r/pull/99"
	prJSON := []byte(`{"url":"` + prURL + `","status":"open"}`)

	task := makeDispatchedTask("feat", "T-merged", "h-merged", "n-merged", "review", 0, "reviewing")
	task.Pr = prJSON

	deps := &fakeReconcilerDeps{bumpReturn: 1}
	r := newTestReconciler(deps, []db.WorkspaceTask{task}, 3, 7200000)

	// Wire isMerged to return true for the task's PR URL.
	r.isMerged = func(_ context.Context, gotURL string) (bool, error) {
		if gotURL != prURL {
			t.Errorf("isMerged called with unexpected URL %q", gotURL)
		}
		return true, nil
	}

	if err := r.reconcile(context.Background(), nil, nil, wsID); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	if len(deps.bumpCalls) != 0 {
		t.Errorf("bumpAttempts should not be called for a merged reviewing task; got %d calls", len(deps.bumpCalls))
	}
	if len(deps.enqueueCalls) != 0 {
		t.Errorf("enqueue should not be called for a merged reviewing task; got %d calls", len(deps.enqueueCalls))
	}
	if len(deps.blockedCalls) != 0 {
		t.Errorf("setBlocked should not be called for a merged reviewing task; got %d calls", len(deps.blockedCalls))
	}
}

// TestReconciler_MergedCheckError_ProceedsToReenqueue verifies that when
// isMerged returns an error, the reconciler logs and proceeds to re-enqueue
// (error is non-fatal — better to re-enqueue than silently drop the task).
func TestReconciler_MergedCheckError_ProceedsToReenqueue(t *testing.T) {
	wsID := uuid.New()
	prURL := "https://github.com/o/r/pull/88"
	prJSON := []byte(`{"url":"` + prURL + `","status":"open"}`)

	task := makeDispatchedTask("feat", "T-err", "h-err", "n-err", "review", 0, "reviewing")
	task.Pr = prJSON

	deps := &fakeReconcilerDeps{bumpReturn: 1}
	r := newTestReconciler(deps, []db.WorkspaceTask{task}, 3, 7200000)

	r.isMerged = func(_ context.Context, _ string) (bool, error) {
		return false, errors.New("github: 503 service unavailable")
	}

	if err := r.reconcile(context.Background(), nil, nil, wsID); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	// Should still re-enqueue despite the API error.
	if len(deps.enqueueCalls) != 1 {
		t.Errorf("enqueue should be called once when isMerged errors; got %d calls", len(deps.enqueueCalls))
	}
}
