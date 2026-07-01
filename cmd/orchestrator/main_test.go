package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tiendv89/workflow-orchestrator/internal/config"
	db "github.com/tiendv89/workflow-orchestrator/internal/database/queries"
	"github.com/tiendv89/workflow-orchestrator/internal/orchestrator"
)

// --- healthz tests ---

func TestHealthzEndpoint(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "ok" {
		t.Errorf("body = %q, want %q", string(body), "ok")
	}
}

// TestStartHealthzServer_BindError verifies that a port-conflict on the healthz
// server does not terminate the process. The goroutine must return cleanly
// (logging the error) rather than calling log.Fatal / os.Exit.
func TestStartHealthzServer_BindError(t *testing.T) {
	// Hold a port so the second bind is guaranteed to fail.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pre-bind: %v", err)
	}
	defer func() { _ = ln.Close() }()
	addr := ln.Addr().String()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		startHealthzServer(ctx, addr) // must log error and return — not call os.Exit
	}()

	select {
	case <-done:
		// goroutine exited cleanly after the bind error — test passes
	case <-time.After(500 * time.Millisecond):
		t.Error("startHealthzServer blocked after bind failure — expected it to log and return")
	}
}

// --- pollState tests ---

func TestPollState_SuccessResetsToBase(t *testing.T) {
	ps := newPollState(10) // base = 10s, maxBackoff = 50s

	// Trigger some backoff first.
	ps.next(true)
	ps.next(true)
	if ps.current == ps.base {
		t.Fatal("expected current to be above base after errors")
	}

	// Success should reset to base.
	ps.next(false)
	if ps.current != ps.base {
		t.Errorf("current = %v after success reset, want %v", ps.current, ps.base)
	}
}

func TestPollState_ErrorDoublesInterval(t *testing.T) {
	ps := newPollState(10) // base = 10s

	ps.next(true) // 10 → 20
	if ps.current != 20*time.Second {
		t.Errorf("after 1 error: current = %v, want 20s", ps.current)
	}

	ps.next(true) // 20 → 40
	if ps.current != 40*time.Second {
		t.Errorf("after 2 errors: current = %v, want 40s", ps.current)
	}
}

func TestPollState_CapsAtMaxBackoff(t *testing.T) {
	ps := newPollState(10) // maxBackoff = 50s

	for i := 0; i < 10; i++ {
		ps.next(true)
	}
	if ps.current != ps.maxBackoff {
		t.Errorf("current = %v after many errors, want maxBackoff = %v", ps.current, ps.maxBackoff)
	}
}

func TestPollState_JitterInRange(t *testing.T) {
	ps := newPollState(10) // base = 10s, jitter ±20% → [8s, 12s]

	for i := 0; i < 100; i++ {
		d := ps.next(false)
		lo := time.Duration(float64(ps.base) * 0.8)
		hi := time.Duration(float64(ps.base) * 1.2)
		if d < lo || d > hi {
			t.Errorf("iter %d: jittered duration %v outside [%v, %v]", i, d, lo, hi)
		}
	}
}

func TestPollState_ErrorJitterInRange(t *testing.T) {
	ps := newPollState(10) // after 1 error: current=20s, jitter → [16s, 24s]

	for i := 0; i < 100; i++ {
		ps.current = ps.base // reset state each iter
		d := ps.next(true)   // forces current → 20s
		lo := time.Duration(float64(20*time.Second) * 0.8)
		hi := time.Duration(float64(20*time.Second) * 1.2)
		if d < lo || d > hi {
			t.Errorf("iter %d: error jitter %v outside [%v, %v]", i, d, lo, hi)
		}
	}
}

// --- runCycle unit tests ---

func minimalCfg() *config.Config {
	return &config.Config{
		WorkspaceID:         uuid.New().String(),
		BrokerURL:           "http://broker.test",
		GitHubToken:         "tok",
		ManagementRepo:      "owner/repo",
		BaseBranch:          "main",
		PollIntervalSeconds: 15,
	}
}

func fixedHandleLC(
	tasks []db.WorkspaceTask,
	findErr error,
	claimWon bool, claimErr error,
	dispatchErr error,
	reapErr error,
	pollErr error,
	handles *[]string,
) loopConfig {
	return loopConfig{
		findEligible: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return tasks, findErr
		},
		claimTask: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID, _, _, _ string) (bool, error) {
			return claimWon, claimErr
		},
		rollbackClaim: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID) (bool, error) {
			return true, nil
		},
		dispatch: func(_ context.Context, _ *config.Config, _ db.WorkspaceTask, handle string) error {
			if handles != nil {
				*handles = append(*handles, handle)
			}
			return dispatchErr
		},
		findReviewable: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return nil, nil
		},
		dispatchReviewer: func(_ context.Context, _ *pgxpool.Pool, _ *config.Config, _ uuid.UUID, _ db.WorkspaceTask, _ *orchestrator.HandleStore) (bool, error) {
			return false, nil
		},
		findFixable: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return nil, nil
		},
		dispatchFix: func(_ context.Context, _ *pgxpool.Pool, _ *config.Config, _ uuid.UUID, _ db.WorkspaceTask, _ *orchestrator.HandleStore) (bool, error) {
			return false, nil
		},
		reapCompleted: func(_ context.Context, _ *config.Config, _ *pgxpool.Pool, _ *orchestrator.HandleStore) error {
			return reapErr
		},
		pollMergedPRs: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) error {
			return pollErr
		},
		newHandle: uuid.New,
	}
}

// TestRunCycle_NoEligibleTasks verifies the cycle completes without error
// when there are no eligible tasks and all other steps succeed.
func TestRunCycle_NoEligibleTasks(t *testing.T) {
	cfg := minimalCfg()
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()
	lc := fixedHandleLC(nil, nil, false, nil, nil, nil, nil, nil)

	hadError := runCycle(context.Background(), cfg, nil, wsID, hs, "test-executor", lc)
	if hadError {
		t.Error("expected hadError=false with no tasks and no errors")
	}
}

// TestRunCycle_FindEligibleError verifies that a FindEligibleTasks error is
// isolated — the cycle continues to reap and merge-poll — and returns true.
func TestRunCycle_FindEligibleError(t *testing.T) {
	cfg := minimalCfg()
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()

	reapCalled := false
	pollCalled := false

	lc := loopConfig{
		findEligible: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return nil, errors.New("db unavailable")
		},
		claimTask: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID, _, _, _ string) (bool, error) {
			return false, nil
		},
		rollbackClaim: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID) (bool, error) {
			return true, nil
		},
		dispatch: func(_ context.Context, _ *config.Config, _ db.WorkspaceTask, _ string) error {
			return nil
		},
		findReviewable: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return nil, nil
		},
		dispatchReviewer: func(_ context.Context, _ *pgxpool.Pool, _ *config.Config, _ uuid.UUID, _ db.WorkspaceTask, _ *orchestrator.HandleStore) (bool, error) {
			return false, nil
		},
		findFixable: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return nil, nil
		},
		dispatchFix: func(_ context.Context, _ *pgxpool.Pool, _ *config.Config, _ uuid.UUID, _ db.WorkspaceTask, _ *orchestrator.HandleStore) (bool, error) {
			return false, nil
		},
		reapCompleted: func(_ context.Context, _ *config.Config, _ *pgxpool.Pool, _ *orchestrator.HandleStore) error {
			reapCalled = true
			return nil
		},
		pollMergedPRs: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) error {
			pollCalled = true
			return nil
		},
		newHandle: uuid.New,
	}

	hadError := runCycle(context.Background(), cfg, nil, wsID, hs, "test-executor", lc)

	if !hadError {
		t.Error("expected hadError=true when FindEligibleTasks fails")
	}
	if !reapCalled {
		t.Error("expected ReapCompleted to be called despite FindEligibleTasks error")
	}
	if !pollCalled {
		t.Error("expected PollMergedPRs to be called despite FindEligibleTasks error")
	}
}

// TestRunCycle_AllStepsError verifies that every step's error is isolated —
// the cycle completes without panicking even when all steps fail.
func TestRunCycle_AllStepsError(t *testing.T) {
	cfg := minimalCfg()
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()

	boom := errors.New("boom")
	lc := fixedHandleLC(nil, boom, false, boom, boom, boom, boom, nil)

	hadError := runCycle(context.Background(), cfg, nil, wsID, hs, "test-executor", lc)
	if !hadError {
		t.Error("expected hadError=true when all steps fail")
	}
}

// TestRunCycle_ClaimLost verifies that a lost claim (won=false) does not
// register the handle in the HandleStore.
func TestRunCycle_ClaimLost(t *testing.T) {
	cfg := minimalCfg()
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()

	task := db.WorkspaceTask{
		TaskID:      uuid.New(),
		FeatureID:   uuid.New(),
		TaskName:    "T1",
		FeatureName: "my-feature",
	}

	var dispatchCalled bool
	var registeredHandles []string

	lc := loopConfig{
		findEligible: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return []db.WorkspaceTask{task}, nil
		},
		claimTask: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID, _, _, _ string) (bool, error) {
			return false, nil // claim lost
		},
		rollbackClaim: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID) (bool, error) {
			return true, nil
		},
		dispatch: func(_ context.Context, _ *config.Config, _ db.WorkspaceTask, handle string) error {
			dispatchCalled = true
			return nil
		},
		findReviewable: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return nil, nil
		},
		dispatchReviewer: func(_ context.Context, _ *pgxpool.Pool, _ *config.Config, _ uuid.UUID, _ db.WorkspaceTask, _ *orchestrator.HandleStore) (bool, error) {
			return false, nil
		},
		findFixable: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return nil, nil
		},
		dispatchFix: func(_ context.Context, _ *pgxpool.Pool, _ *config.Config, _ uuid.UUID, _ db.WorkspaceTask, _ *orchestrator.HandleStore) (bool, error) {
			return false, nil
		},
		reapCompleted: func(_ context.Context, _ *config.Config, _ *pgxpool.Pool, _ *orchestrator.HandleStore) error {
			return nil
		},
		pollMergedPRs: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) error {
			return nil
		},
		newHandle: func() uuid.UUID {
			h := uuid.New()
			registeredHandles = append(registeredHandles, h.String())
			return h
		},
	}

	runCycle(context.Background(), cfg, nil, wsID, hs, "test-executor", lc)

	if dispatchCalled {
		t.Error("Dispatch should not be called when claim is lost")
	}
	// Handle should not be registered.
	for _, h := range registeredHandles {
		if _, ok := hs.Lookup(h); ok {
			t.Errorf("handle %q should not be registered after lost claim", h)
		}
	}
}

// TestRunCycle_DispatchError verifies that a Dispatch error does not register
// the handle in the HandleStore.
func TestRunCycle_DispatchError(t *testing.T) {
	cfg := minimalCfg()
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()

	task := db.WorkspaceTask{
		TaskID:      uuid.New(),
		FeatureID:   uuid.New(),
		TaskName:    "T2",
		FeatureName: "my-feature",
	}

	var usedHandle string

	var rollbackCalled bool

	lc := loopConfig{
		findEligible: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return []db.WorkspaceTask{task}, nil
		},
		claimTask: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID, _, _, _ string) (bool, error) {
			return true, nil // claim won
		},
		rollbackClaim: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID) (bool, error) {
			rollbackCalled = true
			return true, nil
		},
		dispatch: func(_ context.Context, _ *config.Config, _ db.WorkspaceTask, handle string) error {
			usedHandle = handle
			return errors.New("broker unreachable")
		},
		findReviewable: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return nil, nil
		},
		dispatchReviewer: func(_ context.Context, _ *pgxpool.Pool, _ *config.Config, _ uuid.UUID, _ db.WorkspaceTask, _ *orchestrator.HandleStore) (bool, error) {
			return false, nil
		},
		findFixable: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return nil, nil
		},
		dispatchFix: func(_ context.Context, _ *pgxpool.Pool, _ *config.Config, _ uuid.UUID, _ db.WorkspaceTask, _ *orchestrator.HandleStore) (bool, error) {
			return false, nil
		},
		reapCompleted: func(_ context.Context, _ *config.Config, _ *pgxpool.Pool, _ *orchestrator.HandleStore) error {
			return nil
		},
		pollMergedPRs: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) error {
			return nil
		},
		newHandle: uuid.New,
	}

	hadError := runCycle(context.Background(), cfg, nil, wsID, hs, "test-executor", lc)

	if !hadError {
		t.Error("expected hadError=true when dispatch fails")
	}
	if _, ok := hs.Lookup(usedHandle); ok {
		t.Errorf("handle %q should not be registered after dispatch error", usedHandle)
	}
	if !rollbackCalled {
		t.Error("expected RollbackClaim to be called after dispatch error")
	}
}

// TestRunCycle_HappyPath verifies that a successful cycle registers the handle
// in the HandleStore.
func TestRunCycle_HappyPath(t *testing.T) {
	cfg := minimalCfg()
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()

	featureID := uuid.New()
	taskID := uuid.New()
	task := db.WorkspaceTask{
		TaskID:      taskID,
		FeatureID:   featureID,
		TaskName:    "T3",
		FeatureName: "happy-feature",
	}

	fixedHandle := uuid.New()

	lc := loopConfig{
		findEligible: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return []db.WorkspaceTask{task}, nil
		},
		claimTask: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID, _, _, _ string) (bool, error) {
			return true, nil
		},
		rollbackClaim: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID) (bool, error) {
			return true, nil
		},
		dispatch: func(_ context.Context, _ *config.Config, _ db.WorkspaceTask, _ string) error {
			return nil
		},
		findReviewable: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return nil, nil
		},
		dispatchReviewer: func(_ context.Context, _ *pgxpool.Pool, _ *config.Config, _ uuid.UUID, _ db.WorkspaceTask, _ *orchestrator.HandleStore) (bool, error) {
			return false, nil
		},
		findFixable: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return nil, nil
		},
		dispatchFix: func(_ context.Context, _ *pgxpool.Pool, _ *config.Config, _ uuid.UUID, _ db.WorkspaceTask, _ *orchestrator.HandleStore) (bool, error) {
			return false, nil
		},
		reapCompleted: func(_ context.Context, _ *config.Config, _ *pgxpool.Pool, _ *orchestrator.HandleStore) error {
			return nil
		},
		pollMergedPRs: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) error {
			return nil
		},
		newHandle: func() uuid.UUID { return fixedHandle },
	}

	hadError := runCycle(context.Background(), cfg, nil, wsID, hs, "test-executor", lc)
	if hadError {
		t.Error("expected hadError=false on happy path")
	}

	entry, ok := hs.Lookup(fixedHandle.String())
	if !ok {
		t.Fatal("handle not registered in HandleStore after successful dispatch")
	}
	if entry.TaskUUID != taskID {
		t.Errorf("TaskUUID = %v, want %v", entry.TaskUUID, taskID)
	}
	if entry.FeatureUUID != featureID {
		t.Errorf("FeatureUUID = %v, want %v", entry.FeatureUUID, featureID)
	}
	if entry.TaskName != "T3" {
		t.Errorf("TaskName = %q, want %q", entry.TaskName, "T3")
	}
}

// TestRunCycle_ContextCancelled verifies that a cancelled context does not
// cause the cycle to panic.
func TestRunCycle_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond) // let it expire

	cfg := minimalCfg()
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()
	lc := fixedHandleLC(nil, nil, false, nil, nil, nil, nil, nil)

	// Must not panic even with a cancelled context.
	runCycle(ctx, cfg, nil, wsID, hs, "test-executor", lc)
}

// TestRunCycle_ReconcileStuck_Called verifies that reconcileStuck is called each
// cycle when wired, and that its error is isolated (reap and merge-poll still run).
func TestRunCycle_ReconcileStuck_Called(t *testing.T) {
	cfg := minimalCfg()
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()

	reconcileCalled := false
	reapCalled := false
	pollCalled := false

	lc := loopConfig{
		findEligible: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return nil, nil
		},
		claimTask: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID, _, _, _ string) (bool, error) {
			return false, nil
		},
		rollbackClaim: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID) (bool, error) {
			return true, nil
		},
		dispatch: func(_ context.Context, _ *config.Config, _ db.WorkspaceTask, _ string) error {
			return nil
		},
		reapCompleted: func(_ context.Context, _ *config.Config, _ *pgxpool.Pool, _ *orchestrator.HandleStore) error {
			reapCalled = true
			return nil
		},
		reconcileStuck: func(_ context.Context, _ *config.Config, _ *pgxpool.Pool) error {
			reconcileCalled = true
			return errors.New("reconciler error")
		},
		pollMergedPRs: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) error {
			pollCalled = true
			return nil
		},
		newHandle: uuid.New,
	}

	hadError := runCycle(context.Background(), cfg, nil, wsID, hs, "test-executor", lc)

	if !reconcileCalled {
		t.Error("reconcileStuck should have been called")
	}
	if !reapCalled {
		t.Error("reapCompleted should still be called despite reconciler error")
	}
	if !pollCalled {
		t.Error("pollMergedPRs should still be called despite reconciler error")
	}
	if !hadError {
		t.Error("hadError should be true when reconcileStuck returns an error")
	}
}

// TestRunCycle_ReconcileStuck_Nil verifies that a nil reconcileStuck is safely
// skipped — existing callers that don't wire it still work.
func TestRunCycle_ReconcileStuck_Nil(t *testing.T) {
	cfg := minimalCfg()
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()
	lc := fixedHandleLC(nil, nil, false, nil, nil, nil, nil, nil)
	// reconcileStuck is nil in fixedHandleLC — must not panic

	hadError := runCycle(context.Background(), cfg, nil, wsID, hs, "test-executor", lc)
	if hadError {
		t.Error("expected hadError=false with no tasks and no errors")
	}
}

// TestRunCycle_ConflictDispatch_Called verifies that the conflict dispatch step
// (Step g) is executed when findConflicted and dispatchConflicted are wired,
// and that it is called for each conflicted task returned.
func TestRunCycle_ConflictDispatch_Called(t *testing.T) {
	cfg := minimalCfg()
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()

	conflictDispatchCalled := false

	lc := loopConfig{
		findEligible: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return nil, nil
		},
		claimTask: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID, _, _, _ string) (bool, error) {
			return false, nil
		},
		rollbackClaim: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID) (bool, error) {
			return true, nil
		},
		dispatch: func(_ context.Context, _ *config.Config, _ db.WorkspaceTask, _ string) error {
			return nil
		},
		findConflicted: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return []db.WorkspaceTask{{TaskName: "T8", FeatureName: "go-orchestrator-autonomy"}}, nil
		},
		dispatchConflicted: func(_ context.Context, _ *pgxpool.Pool, _ *config.Config, _ uuid.UUID, _ db.WorkspaceTask, _ *orchestrator.HandleStore) (bool, error) {
			conflictDispatchCalled = true
			return true, nil
		},
		reapCompleted: func(_ context.Context, _ *config.Config, _ *pgxpool.Pool, _ *orchestrator.HandleStore) error {
			return nil
		},
		pollMergedPRs: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) error {
			return nil
		},
		newHandle: uuid.New,
	}

	hadError := runCycle(context.Background(), cfg, nil, wsID, hs, "test-executor", lc)
	if hadError {
		t.Error("expected hadError=false on conflict dispatch happy path")
	}
	if !conflictDispatchCalled {
		t.Error("expected dispatchConflicted to be called for conflicted task")
	}
}

// TestRunCycle_ConflictDispatch_Nil verifies that nil findConflicted/dispatchConflicted
// are safely skipped — existing callers that don't wire them still work.
func TestRunCycle_ConflictDispatch_Nil(t *testing.T) {
	cfg := minimalCfg()
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()
	lc := fixedHandleLC(nil, nil, false, nil, nil, nil, nil, nil)
	// findConflicted and dispatchConflicted are nil in fixedHandleLC — must not panic

	hadError := runCycle(context.Background(), cfg, nil, wsID, hs, "test-executor", lc)
	if hadError {
		t.Error("expected hadError=false with nil conflict dispatch functions")
	}
}

// TestRunCycle_ConflictDispatch_Error verifies that a dispatchConflicted error
// is isolated — the cycle continues and returns hadError=true.
func TestRunCycle_ConflictDispatch_Error(t *testing.T) {
	cfg := minimalCfg()
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()

	lc := loopConfig{
		findEligible: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return nil, nil
		},
		claimTask: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID, _, _, _ string) (bool, error) {
			return false, nil
		},
		rollbackClaim: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID) (bool, error) {
			return true, nil
		},
		dispatch: func(_ context.Context, _ *config.Config, _ db.WorkspaceTask, _ string) error {
			return nil
		},
		findConflicted: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return []db.WorkspaceTask{{TaskName: "T8", FeatureName: "go-orchestrator-autonomy"}}, nil
		},
		dispatchConflicted: func(_ context.Context, _ *pgxpool.Pool, _ *config.Config, _ uuid.UUID, _ db.WorkspaceTask, _ *orchestrator.HandleStore) (bool, error) {
			return false, errors.New("broker unavailable")
		},
		reapCompleted: func(_ context.Context, _ *config.Config, _ *pgxpool.Pool, _ *orchestrator.HandleStore) error {
			return nil
		},
		pollMergedPRs: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) error {
			return nil
		},
		newHandle: uuid.New,
	}

	hadError := runCycle(context.Background(), cfg, nil, wsID, hs, "test-executor", lc)
	if !hadError {
		t.Error("expected hadError=true when dispatchConflicted fails")
	}
}

// --- Soft-claim throttle tests ---

// TestRunCycle_Throttle_ZeroHeadroomBlocksAllDispatch verifies that when the
// in-flight count equals MAX_INFLIGHT, no claim/reviewer/fix/rebase dispatch
// occurs in that cycle.
func TestRunCycle_Throttle_ZeroHeadroomBlocksAllDispatch(t *testing.T) {
	cfg := &config.Config{
		WorkspaceID:         uuid.New().String(),
		BrokerURL:           "http://broker.test",
		GitHubToken:         "tok",
		ManagementRepo:      "owner/repo",
		BaseBranch:          "main",
		PollIntervalSeconds: 15,
		MaxInFlight:         2, // already full
	}
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()

	claimCalled := false
	reviewerCalled := false
	fixCalled := false
	conflictCalled := false

	task := db.WorkspaceTask{TaskID: uuid.New(), FeatureID: uuid.New(), TaskName: "T1", FeatureName: "f"}

	lc := loopConfig{
		countInFlight: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) (int, error) {
			return 2, nil // inflight == MAX_INFLIGHT → headroom = 0
		},
		findEligible: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return []db.WorkspaceTask{task}, nil
		},
		claimTask: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID, _, _, _ string) (bool, error) {
			claimCalled = true
			return true, nil
		},
		rollbackClaim: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID) (bool, error) {
			return true, nil
		},
		dispatch: func(_ context.Context, _ *config.Config, _ db.WorkspaceTask, _ string) error {
			return nil
		},
		findReviewable: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return []db.WorkspaceTask{task}, nil
		},
		dispatchReviewer: func(_ context.Context, _ *pgxpool.Pool, _ *config.Config, _ uuid.UUID, _ db.WorkspaceTask, _ *orchestrator.HandleStore) (bool, error) {
			reviewerCalled = true
			return true, nil
		},
		findFixable: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return []db.WorkspaceTask{task}, nil
		},
		dispatchFix: func(_ context.Context, _ *pgxpool.Pool, _ *config.Config, _ uuid.UUID, _ db.WorkspaceTask, _ *orchestrator.HandleStore) (bool, error) {
			fixCalled = true
			return true, nil
		},
		findConflictedHandoffPRs: func(_ context.Context, _ *pgxpool.Pool, _ int) ([]db.HandoffPr, error) {
			return nil, nil
		},
		findConflicted: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return []db.WorkspaceTask{task}, nil
		},
		dispatchConflicted: func(_ context.Context, _ *pgxpool.Pool, _ *config.Config, _ uuid.UUID, _ db.WorkspaceTask, _ *orchestrator.HandleStore) (bool, error) {
			conflictCalled = true
			return true, nil
		},
		reapCompleted: func(_ context.Context, _ *config.Config, _ *pgxpool.Pool, _ *orchestrator.HandleStore) error {
			return nil
		},
		pollMergedPRs: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) error {
			return nil
		},
		newHandle: uuid.New,
	}

	hadError := runCycle(context.Background(), cfg, nil, wsID, hs, "test-executor", lc)
	if hadError {
		t.Error("expected hadError=false — throttle skip is not an error")
	}
	if claimCalled {
		t.Error("claim should not be called when headroom is 0")
	}
	if reviewerCalled {
		t.Error("reviewer should not be called when headroom is 0")
	}
	if fixCalled {
		t.Error("fix should not be called when headroom is 0")
	}
	if conflictCalled {
		t.Error("task rebase should not be called when headroom is 0")
	}
}

// TestRunCycle_Throttle_HeadroomLimitsDispatches verifies that exactly
// MAX_INFLIGHT - inflight dispatches are made across all dispatch kinds.
func TestRunCycle_Throttle_HeadroomLimitsDispatches(t *testing.T) {
	cfg := &config.Config{
		WorkspaceID:         uuid.New().String(),
		BrokerURL:           "http://broker.test",
		GitHubToken:         "tok",
		ManagementRepo:      "owner/repo",
		BaseBranch:          "main",
		PollIntervalSeconds: 15,
		MaxInFlight:         3, // inflight=1 → headroom=2
	}
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()

	dispatchCount := 0

	mkTask := func(name string) db.WorkspaceTask {
		return db.WorkspaceTask{TaskID: uuid.New(), FeatureID: uuid.New(), TaskName: name, FeatureName: "f"}
	}
	tasks := []db.WorkspaceTask{mkTask("T1"), mkTask("T2"), mkTask("T3"), mkTask("T4")}

	lc := loopConfig{
		countInFlight: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) (int, error) {
			return 1, nil // headroom = 3 - 1 = 2
		},
		findEligible: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return tasks, nil
		},
		claimTask: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID, _, _, _ string) (bool, error) {
			return true, nil
		},
		rollbackClaim: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID) (bool, error) {
			return true, nil
		},
		dispatch: func(_ context.Context, _ *config.Config, _ db.WorkspaceTask, _ string) error {
			dispatchCount++
			return nil
		},
		reapCompleted: func(_ context.Context, _ *config.Config, _ *pgxpool.Pool, _ *orchestrator.HandleStore) error {
			return nil
		},
		pollMergedPRs: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) error {
			return nil
		},
		newHandle: uuid.New,
	}

	hadError := runCycle(context.Background(), cfg, nil, wsID, hs, "test-executor", lc)
	if hadError {
		t.Error("expected hadError=false")
	}
	if dispatchCount != 2 {
		t.Errorf("dispatched %d tasks, want 2 (headroom=2)", dispatchCount)
	}
}

// TestRunCycle_Throttle_MultiInstanceOvershotBounded verifies that when inflight
// already exceeds MAX_INFLIGHT (multi-instance race), headroom is 0 and no new
// dispatches occur (overshoot is bounded, not compounded).
func TestRunCycle_Throttle_MultiInstanceOvershotBounded(t *testing.T) {
	cfg := &config.Config{
		WorkspaceID:         uuid.New().String(),
		BrokerURL:           "http://broker.test",
		GitHubToken:         "tok",
		ManagementRepo:      "owner/repo",
		BaseBranch:          "main",
		PollIntervalSeconds: 15,
		MaxInFlight:         3,
	}
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()

	claimCalled := false
	task := db.WorkspaceTask{TaskID: uuid.New(), FeatureID: uuid.New(), TaskName: "T1", FeatureName: "f"}

	lc := loopConfig{
		countInFlight: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) (int, error) {
			return 5, nil // inflight > MAX_INFLIGHT — overshoot scenario
		},
		findEligible: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return []db.WorkspaceTask{task}, nil
		},
		claimTask: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID, _, _, _ string) (bool, error) {
			claimCalled = true
			return true, nil
		},
		rollbackClaim: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID) (bool, error) {
			return true, nil
		},
		dispatch: func(_ context.Context, _ *config.Config, _ db.WorkspaceTask, _ string) error {
			return nil
		},
		reapCompleted: func(_ context.Context, _ *config.Config, _ *pgxpool.Pool, _ *orchestrator.HandleStore) error {
			return nil
		},
		pollMergedPRs: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) error {
			return nil
		},
		newHandle: uuid.New,
	}

	hadError := runCycle(context.Background(), cfg, nil, wsID, hs, "test-executor", lc)
	if hadError {
		t.Error("expected hadError=false — overshoot detected but not an error")
	}
	if claimCalled {
		t.Error("claim should not be called when inflight > MAX_INFLIGHT")
	}
}

// TestRunCycle_Throttle_CountInFlightError verifies that a CountInFlight error
// skips all dispatch kinds (safe-fail) and returns hadError=true. Reap and
// reconcile still run.
func TestRunCycle_Throttle_CountInFlightError(t *testing.T) {
	cfg := &config.Config{
		WorkspaceID:         uuid.New().String(),
		BrokerURL:           "http://broker.test",
		GitHubToken:         "tok",
		ManagementRepo:      "owner/repo",
		BaseBranch:          "main",
		PollIntervalSeconds: 15,
		MaxInFlight:         5,
	}
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()

	claimCalled := false
	reapCalled := false
	reconcileCalled := false

	task := db.WorkspaceTask{TaskID: uuid.New(), FeatureID: uuid.New(), TaskName: "T1", FeatureName: "f"}

	lc := loopConfig{
		countInFlight: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) (int, error) {
			return 0, errors.New("db unavailable")
		},
		findEligible: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return []db.WorkspaceTask{task}, nil
		},
		claimTask: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID, _, _, _ string) (bool, error) {
			claimCalled = true
			return true, nil
		},
		rollbackClaim: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID) (bool, error) {
			return true, nil
		},
		dispatch: func(_ context.Context, _ *config.Config, _ db.WorkspaceTask, _ string) error {
			return nil
		},
		reapCompleted: func(_ context.Context, _ *config.Config, _ *pgxpool.Pool, _ *orchestrator.HandleStore) error {
			reapCalled = true
			return nil
		},
		reconcileStuck: func(_ context.Context, _ *config.Config, _ *pgxpool.Pool) error {
			reconcileCalled = true
			return nil
		},
		pollMergedPRs: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) error {
			return nil
		},
		newHandle: uuid.New,
	}

	hadError := runCycle(context.Background(), cfg, nil, wsID, hs, "test-executor", lc)
	if !hadError {
		t.Error("expected hadError=true when CountInFlight fails")
	}
	if claimCalled {
		t.Error("claim should not be called when CountInFlight returns an error")
	}
	if !reapCalled {
		t.Error("reap should still run even when CountInFlight fails")
	}
	if !reconcileCalled {
		t.Error("reconcile should still run even when CountInFlight fails")
	}
}

// TestRunCycle_CycleOrder_HandoffRebaseBeforeTaskDispatch verifies that
// handoff-PR rebases (HIGH priority) run before task claim dispatch.
func TestRunCycle_CycleOrder_HandoffRebaseBeforeTaskDispatch(t *testing.T) {
	cfg := &config.Config{
		WorkspaceID:         uuid.New().String(),
		BrokerURL:           "http://broker.test",
		GitHubToken:         "tok",
		ManagementRepo:      "owner/repo",
		BaseBranch:          "main",
		PollIntervalSeconds: 15,
		MaxInFlight:         5,
	}
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()

	var order []string

	task := db.WorkspaceTask{TaskID: uuid.New(), FeatureID: uuid.New(), TaskName: "T1", FeatureName: "f"}
	hpr := db.HandoffPr{ID: uuid.New(), Repo: "workflow-backend"}

	lc := loopConfig{
		countInFlight: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) (int, error) {
			return 0, nil
		},
		findConflictedHandoffPRs: func(_ context.Context, _ *pgxpool.Pool, _ int) ([]db.HandoffPr, error) {
			return []db.HandoffPr{hpr}, nil
		},
		dispatchHandoffPRRebase: func(_ context.Context, _ *pgxpool.Pool, _ *config.Config, _ uuid.UUID, _ db.HandoffPr, _ *orchestrator.HandleStore) (bool, error) {
			order = append(order, "handoff-rebase")
			return true, nil
		},
		findEligible: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return []db.WorkspaceTask{task}, nil
		},
		claimTask: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID, _, _, _ string) (bool, error) {
			order = append(order, "task-claim")
			return true, nil
		},
		rollbackClaim: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID) (bool, error) {
			return true, nil
		},
		dispatch: func(_ context.Context, _ *config.Config, _ db.WorkspaceTask, _ string) error {
			return nil
		},
		reapCompleted: func(_ context.Context, _ *config.Config, _ *pgxpool.Pool, _ *orchestrator.HandleStore) error {
			order = append(order, "reap")
			return nil
		},
		pollMergedPRs: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) error {
			order = append(order, "poll-merged-prs")
			return nil
		},
		newHandle: uuid.New,
	}

	runCycle(context.Background(), cfg, nil, wsID, hs, "test-executor", lc)

	// Expected order: handoff-rebase → task-claim → poll-merged-prs → reap
	if len(order) < 4 {
		t.Fatalf("expected at least 4 steps recorded, got %d: %v", len(order), order)
	}
	handoffIdx, claimIdx, pollIdx, reapIdx := -1, -1, -1, -1
	for i, s := range order {
		switch s {
		case "handoff-rebase":
			handoffIdx = i
		case "task-claim":
			claimIdx = i
		case "poll-merged-prs":
			pollIdx = i
		case "reap":
			reapIdx = i
		}
	}
	if handoffIdx == -1 || claimIdx == -1 || pollIdx == -1 || reapIdx == -1 {
		t.Fatalf("missing step in order: %v", order)
	}
	if handoffIdx >= claimIdx {
		t.Errorf("handoff-rebase (%d) must run before task-claim (%d), order: %v", handoffIdx, claimIdx, order)
	}
	if claimIdx >= pollIdx {
		t.Errorf("task-claim (%d) must run before poll-merged-prs (%d), order: %v", claimIdx, pollIdx, order)
	}
	if pollIdx >= reapIdx {
		t.Errorf("poll-merged-prs (%d) must run before reap (%d), order: %v", pollIdx, reapIdx, order)
	}
}

// TestRunCycle_CycleOrder_TaskRebaseBeforeReap verifies that task-rebase dispatch
// runs after poll-merged-PRs but before reap, so newly-conflicted tasks are
// picked up within the same cycle.
func TestRunCycle_CycleOrder_TaskRebaseBeforeReap(t *testing.T) {
	cfg := minimalCfg()
	cfg.MaxInFlight = 5
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()

	var order []string

	task := db.WorkspaceTask{TaskID: uuid.New(), FeatureID: uuid.New(), TaskName: "T1", FeatureName: "f"}

	lc := loopConfig{
		findEligible: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return nil, nil
		},
		claimTask: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID, _, _, _ string) (bool, error) {
			return false, nil
		},
		rollbackClaim: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID) (bool, error) {
			return true, nil
		},
		dispatch: func(_ context.Context, _ *config.Config, _ db.WorkspaceTask, _ string) error {
			return nil
		},
		findConflicted: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return []db.WorkspaceTask{task}, nil
		},
		dispatchConflicted: func(_ context.Context, _ *pgxpool.Pool, _ *config.Config, _ uuid.UUID, _ db.WorkspaceTask, _ *orchestrator.HandleStore) (bool, error) {
			order = append(order, "task-rebase")
			return true, nil
		},
		reapCompleted: func(_ context.Context, _ *config.Config, _ *pgxpool.Pool, _ *orchestrator.HandleStore) error {
			order = append(order, "reap")
			return nil
		},
		pollMergedPRs: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) error {
			order = append(order, "poll-merged-prs")
			return nil
		},
		newHandle: uuid.New,
	}

	runCycle(context.Background(), cfg, nil, wsID, hs, "test-executor", lc)

	rebaseIdx, pollIdx, reapIdx := -1, -1, -1
	for i, s := range order {
		switch s {
		case "task-rebase":
			rebaseIdx = i
		case "poll-merged-prs":
			pollIdx = i
		case "reap":
			reapIdx = i
		}
	}
	if rebaseIdx == -1 || pollIdx == -1 || reapIdx == -1 {
		t.Fatalf("missing step in order: %v", order)
	}
	if pollIdx >= rebaseIdx {
		t.Errorf("poll-merged-prs (%d) must run before task-rebase (%d), order: %v", pollIdx, rebaseIdx, order)
	}
	if rebaseIdx >= reapIdx {
		t.Errorf("task-rebase (%d) must run before reap (%d), order: %v", rebaseIdx, reapIdx, order)
	}
}

// TestRunCycle_Throttle_HandoffRebaseConsumesBudget verifies that handoff-PR
// rebases consume the shared headroom before task dispatch.
func TestRunCycle_Throttle_HandoffRebaseConsumesBudget(t *testing.T) {
	cfg := &config.Config{
		WorkspaceID:         uuid.New().String(),
		BrokerURL:           "http://broker.test",
		GitHubToken:         "tok",
		ManagementRepo:      "owner/repo",
		BaseBranch:          "main",
		PollIntervalSeconds: 15,
		MaxInFlight:         2,
	}
	hs := orchestrator.NewHandleStore()
	wsID := uuid.New()

	taskClaimCalled := false
	task := db.WorkspaceTask{TaskID: uuid.New(), FeatureID: uuid.New(), TaskName: "T1", FeatureName: "f"}
	hpr1 := db.HandoffPr{ID: uuid.New(), Repo: "repo-a"}
	hpr2 := db.HandoffPr{ID: uuid.New(), Repo: "repo-b"}

	lc := loopConfig{
		countInFlight: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) (int, error) {
			return 0, nil // headroom = 2
		},
		findConflictedHandoffPRs: func(_ context.Context, _ *pgxpool.Pool, _ int) ([]db.HandoffPr, error) {
			return []db.HandoffPr{hpr1, hpr2}, nil // 2 handoff rebases exhaust headroom
		},
		dispatchHandoffPRRebase: func(_ context.Context, _ *pgxpool.Pool, _ *config.Config, _ uuid.UUID, _ db.HandoffPr, _ *orchestrator.HandleStore) (bool, error) {
			return true, nil
		},
		findEligible: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) ([]db.WorkspaceTask, error) {
			return []db.WorkspaceTask{task}, nil
		},
		claimTask: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID, _, _, _ string) (bool, error) {
			taskClaimCalled = true
			return true, nil
		},
		rollbackClaim: func(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID) (bool, error) {
			return true, nil
		},
		dispatch: func(_ context.Context, _ *config.Config, _ db.WorkspaceTask, _ string) error {
			return nil
		},
		reapCompleted: func(_ context.Context, _ *config.Config, _ *pgxpool.Pool, _ *orchestrator.HandleStore) error {
			return nil
		},
		pollMergedPRs: func(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID) error {
			return nil
		},
		newHandle: uuid.New,
	}

	hadError := runCycle(context.Background(), cfg, nil, wsID, hs, "test-executor", lc)
	if hadError {
		t.Error("expected hadError=false")
	}
	if taskClaimCalled {
		t.Error("task claim should not be called — headroom exhausted by handoff rebases")
	}
}
