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
