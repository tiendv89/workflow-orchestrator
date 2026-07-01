package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --- helpers ---

func makeCompletionResponse(records []completionRecord) []byte {
	b, _ := json.Marshal(records)
	return b
}

// fakeTransition captures calls to SetInReview / SetBlocked / SetReadyFromMaxTurns.
type fakeTransition struct {
	mu            sync.Mutex
	inReviewCalls []struct {
		workspaceID uuid.UUID
		taskUUID    uuid.UUID
		prURL       string
	}
	blockedCalls []struct {
		workspaceID uuid.UUID
		taskUUID    uuid.UUID
		reason      string
		fromStatus  string
	}
	readyMaxTurnsCalls []struct {
		workspaceID uuid.UUID
		taskUUID    uuid.UUID
	}
	logCalls []struct {
		action string
		note   string
	}
	slowLookupResult *HandleEntry
	slowLookupErr    error
	maxTurnsCount    int32 // returned by getMaxTurnsCount
}

func (f *fakeTransition) setInReview(_ context.Context, _ *pgxpool.Pool, workspaceID, taskUUID uuid.UUID, prURL string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inReviewCalls = append(f.inReviewCalls, struct {
		workspaceID uuid.UUID
		taskUUID    uuid.UUID
		prURL       string
	}{workspaceID, taskUUID, prURL})
	return true, nil
}

func (f *fakeTransition) setBlocked(_ context.Context, _ *pgxpool.Pool, workspaceID, taskUUID uuid.UUID, reason, fromStatus string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blockedCalls = append(f.blockedCalls, struct {
		workspaceID uuid.UUID
		taskUUID    uuid.UUID
		reason      string
		fromStatus  string
	}{workspaceID, taskUUID, reason, fromStatus})
	return true, nil
}

func (f *fakeTransition) setReadyMaxTurns(_ context.Context, _ *pgxpool.Pool, workspaceID, taskUUID uuid.UUID) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readyMaxTurnsCalls = append(f.readyMaxTurnsCalls, struct {
		workspaceID uuid.UUID
		taskUUID    uuid.UUID
	}{workspaceID, taskUUID})
	return true, nil
}

func (f *fakeTransition) getMaxTurnsCount(_ context.Context, _ *pgxpool.Pool, _, _ uuid.UUID) (int32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxTurnsCount, nil
}

func (f *fakeTransition) lookupBySlug(_ context.Context, _ *pgxpool.Pool, _ uuid.UUID, _, _ string) (*HandleEntry, error) {
	return f.slowLookupResult, f.slowLookupErr
}

func (f *fakeTransition) appendLog(_ context.Context, _ *pgxpool.Pool, _, _, _ uuid.UUID, action, _, note string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logCalls = append(f.logCalls, struct {
		action string
		note   string
	}{action, note})
	return nil
}

// newTestReaper wires a Reaper with a mock broker server and fake transitions.
// executorMaxRetries defaults to 3 if 0 is passed.
func newTestReaper(srv *httptest.Server, ft *fakeTransition) *Reaper {
	return newTestReaperWithRetries(srv, ft, 3)
}

func newTestReaperWithRetries(srv *httptest.Server, ft *fakeTransition, executorMaxRetries int) *Reaper {
	return &Reaper{
		brokerURL:          srv.URL,
		httpClient:         srv.Client(),
		setInReview:        ft.setInReview,
		setBlocked:         ft.setBlocked,
		setReadyMaxTurns:   ft.setReadyMaxTurns,
		getMaxTurnsCount:   ft.getMaxTurnsCount,
		slowLookup:         ft.lookupBySlug,
		appendLog:          ft.appendLog,
		executorMaxRetries: executorMaxRetries,
	}
}

// ackOK writes a 204 response for POST /ack requests.
func ackOK(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path == "/ack" && r.Method == http.MethodPost {
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
}

// --- tests ---

// TestReap_GoCompletion_InReview verifies that a go completion with
// terminal_status "in_review" transitions the task and deletes the handle.
func TestReap_GoCompletion_InReview(t *testing.T) {
	taskUUID := uuid.New()
	featureUUID := uuid.New()
	workspaceUUID := uuid.New()

	completion := completionRecord{
		Handle: "handle-001",
		Metadata: handleMetadata{
			FeatureID: "my-feature",
			TaskID:    "T1",
		},
		Result: executorResult{
			TerminalStatus: "in_review",
			PrURL:          "https://github.com/owner/repo/pull/42",
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ackOK(w, r) {
			return
		}
		if r.URL.Path != "/list-completed" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.Method)
		}
		var reqBody struct {
			Owner string `json:"owner"`
		}
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		if reqBody.Owner != "go" {
			t.Errorf("owner = %q, want go", reqBody.Owner)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(makeCompletionResponse([]completionRecord{completion}))
	}))
	defer srv.Close()

	ft := &fakeTransition{}
	hs := NewHandleStore()
	hs.Register("handle-001", HandleEntry{
		FeatureUUID: featureUUID,
		TaskUUID:    taskUUID,
		FeatureName: "my-feature",
		TaskName:    "T1",
	})

	r := newTestReaper(srv, ft)
	if err := r.reap(context.Background(), nil, hs, workspaceUUID); err != nil {
		t.Fatalf("reap error: %v", err)
	}

	// Assert SetInReview was called once with the correct args.
	ft.mu.Lock()
	defer ft.mu.Unlock()
	if len(ft.inReviewCalls) != 1 {
		t.Fatalf("SetInReview called %d times, want 1", len(ft.inReviewCalls))
	}
	call := ft.inReviewCalls[0]
	if call.taskUUID != taskUUID {
		t.Errorf("taskUUID = %v, want %v", call.taskUUID, taskUUID)
	}
	if call.prURL != "https://github.com/owner/repo/pull/42" {
		t.Errorf("prURL = %q, want the PR URL", call.prURL)
	}

	// Assert handle was deleted from the store.
	if _, found := hs.Lookup("handle-001"); found {
		t.Error("handle-001 still in store after reap; want deleted")
	}

	// Assert log entry was appended.
	if len(ft.logCalls) != 1 {
		t.Errorf("appendLog called %d times, want 1", len(ft.logCalls))
	}
	if ft.logCalls[0].action != "reap" {
		t.Errorf("log action = %q, want reap", ft.logCalls[0].action)
	}
}

// TestReap_GoCompletion_Blocked_ImplKind verifies that a go completion with
// terminal_status "blocked" from an impl dispatch records blocked_from_status="in_progress".
func TestReap_GoCompletion_Blocked_ImplKind(t *testing.T) {
	taskUUID := uuid.New()
	featureUUID := uuid.New()
	workspaceUUID := uuid.New()

	completion := completionRecord{
		Handle: "handle-002",
		Metadata: handleMetadata{
			Kind:      "impl",
			FeatureID: "feat",
			TaskID:    "T2",
		},
		Result: executorResult{
			TerminalStatus: "blocked",
			BlockedReason:  "tests_failed",
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ackOK(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(makeCompletionResponse([]completionRecord{completion}))
	}))
	defer srv.Close()

	ft := &fakeTransition{}
	hs := NewHandleStore()
	hs.Register("handle-002", HandleEntry{
		FeatureUUID: featureUUID,
		TaskUUID:    taskUUID,
		FeatureName: "feat",
		TaskName:    "T2",
	})

	r := newTestReaper(srv, ft)
	if err := r.reap(context.Background(), nil, hs, workspaceUUID); err != nil {
		t.Fatalf("reap error: %v", err)
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()
	if len(ft.blockedCalls) != 1 {
		t.Fatalf("SetBlocked called %d times, want 1", len(ft.blockedCalls))
	}
	call := ft.blockedCalls[0]
	if call.taskUUID != taskUUID {
		t.Errorf("taskUUID = %v, want %v", call.taskUUID, taskUUID)
	}
	if call.reason != "tests_failed" {
		t.Errorf("reason = %q, want tests_failed", call.reason)
	}
	if call.fromStatus != "in_progress" {
		t.Errorf("fromStatus = %q, want in_progress for impl dispatch kind", call.fromStatus)
	}

	if _, found := hs.Lookup("handle-002"); found {
		t.Error("handle-002 still in store after reap; want deleted")
	}
}

// TestReap_GoCompletion_Blocked_ReviewKind verifies that a go completion with
// terminal_status "blocked" from a review dispatch records blocked_from_status="reviewing".
func TestReap_GoCompletion_Blocked_ReviewKind(t *testing.T) {
	taskUUID := uuid.New()
	featureUUID := uuid.New()
	workspaceUUID := uuid.New()

	completion := completionRecord{
		Handle: "handle-review-blocked",
		Metadata: handleMetadata{
			Kind:      "review",
			FeatureID: "feat",
			TaskID:    "T3",
		},
		Result: executorResult{
			TerminalStatus: "blocked",
			BlockedReason:  "missing_tool",
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ackOK(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(makeCompletionResponse([]completionRecord{completion}))
	}))
	defer srv.Close()

	ft := &fakeTransition{}
	hs := NewHandleStore()
	hs.Register("handle-review-blocked", HandleEntry{
		FeatureUUID: featureUUID,
		TaskUUID:    taskUUID,
		FeatureName: "feat",
		TaskName:    "T3",
	})

	r := newTestReaper(srv, ft)
	if err := r.reap(context.Background(), nil, hs, workspaceUUID); err != nil {
		t.Fatalf("reap error: %v", err)
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()
	if len(ft.blockedCalls) != 1 {
		t.Fatalf("SetBlocked called %d times, want 1", len(ft.blockedCalls))
	}
	call := ft.blockedCalls[0]
	if call.fromStatus != "reviewing" {
		t.Errorf("fromStatus = %q, want reviewing for review dispatch kind", call.fromStatus)
	}
	if call.reason != "missing_tool" {
		t.Errorf("reason = %q, want missing_tool", call.reason)
	}
}

// TestReap_GoCompletion_Blocked_FixKind verifies that a fix dispatch blocked
// completion records blocked_from_status="in_progress".
func TestReap_GoCompletion_Blocked_FixKind(t *testing.T) {
	taskUUID := uuid.New()
	featureUUID := uuid.New()
	workspaceUUID := uuid.New()

	completion := completionRecord{
		Handle: "handle-fix-blocked",
		Metadata: handleMetadata{
			Kind:      "fix",
			FeatureID: "feat",
			TaskID:    "T4",
		},
		Result: executorResult{
			TerminalStatus: "blocked",
			BlockedReason:  "handover_for_continuation",
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ackOK(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(makeCompletionResponse([]completionRecord{completion}))
	}))
	defer srv.Close()

	ft := &fakeTransition{}
	hs := NewHandleStore()
	hs.Register("handle-fix-blocked", HandleEntry{
		FeatureUUID: featureUUID,
		TaskUUID:    taskUUID,
		FeatureName: "feat",
		TaskName:    "T4",
	})

	r := newTestReaper(srv, ft)
	if err := r.reap(context.Background(), nil, hs, workspaceUUID); err != nil {
		t.Fatalf("reap error: %v", err)
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()
	if len(ft.blockedCalls) != 1 {
		t.Fatalf("SetBlocked called %d times, want 1", len(ft.blockedCalls))
	}
	if ft.blockedCalls[0].fromStatus != "in_progress" {
		t.Errorf("fromStatus = %q, want in_progress for fix dispatch kind", ft.blockedCalls[0].fromStatus)
	}
}

// TestReap_UnknownHandle verifies that a completion whose handle is not in the
// HandleStore or DB logs a warning and does not crash the loop.
func TestReap_UnknownHandle(t *testing.T) {
	workspaceUUID := uuid.New()

	completion := completionRecord{
		Handle: "handle-unknown",
		Metadata: handleMetadata{
			FeatureID: "unknown-feat",
			TaskID:    "T99",
		},
		Result: executorResult{
			TerminalStatus: "in_review",
			PrURL:          "https://github.com/owner/repo/pull/1",
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(makeCompletionResponse([]completionRecord{completion}))
	}))
	defer srv.Close()

	ft := &fakeTransition{slowLookupResult: nil} // DB slow path also returns nil
	hs := NewHandleStore()                       // empty store — handle not registered

	r := newTestReaper(srv, ft)
	if err := r.reap(context.Background(), nil, hs, workspaceUUID); err != nil {
		t.Fatalf("reap returned unexpected error: %v", err)
	}

	// No transitions should have been applied.
	ft.mu.Lock()
	defer ft.mu.Unlock()
	if len(ft.inReviewCalls) != 0 {
		t.Errorf("SetInReview called %d times, want 0", len(ft.inReviewCalls))
	}
	if len(ft.blockedCalls) != 0 {
		t.Errorf("SetBlocked called %d times, want 0", len(ft.blockedCalls))
	}
}

// TestReap_SlowPathResolution verifies the DB slow path is used when the handle
// is not in the HandleStore.
func TestReap_SlowPathResolution(t *testing.T) {
	taskUUID := uuid.New()
	featureUUID := uuid.New()
	workspaceUUID := uuid.New()

	completion := completionRecord{
		Handle: "handle-slow",
		Metadata: handleMetadata{
			FeatureID: "slow-feat",
			TaskID:    "T5",
		},
		Result: executorResult{
			TerminalStatus: "in_review",
			PrURL:          "https://github.com/owner/repo/pull/7",
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(makeCompletionResponse([]completionRecord{completion}))
	}))
	defer srv.Close()

	// Slow path returns a resolved entry.
	resolvedEntry := &HandleEntry{
		FeatureUUID: featureUUID,
		TaskUUID:    taskUUID,
		FeatureName: "slow-feat",
		TaskName:    "T5",
	}
	ft := &fakeTransition{slowLookupResult: resolvedEntry}
	hs := NewHandleStore() // empty — forces slow path

	r := newTestReaper(srv, ft)
	if err := r.reap(context.Background(), nil, hs, workspaceUUID); err != nil {
		t.Fatalf("reap error: %v", err)
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()
	if len(ft.inReviewCalls) != 1 {
		t.Fatalf("SetInReview called %d times, want 1", len(ft.inReviewCalls))
	}
	if ft.inReviewCalls[0].taskUUID != taskUUID {
		t.Errorf("taskUUID = %v, want %v", ft.inReviewCalls[0].taskUUID, taskUUID)
	}
}

// TestReap_EmptyQueue verifies that an empty broker response is handled without error.
func TestReap_EmptyQueue(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	defer srv.Close()

	ft := &fakeTransition{}
	hs := NewHandleStore()

	r := newTestReaper(srv, ft)
	if err := r.reap(context.Background(), nil, hs, uuid.New()); err != nil {
		t.Fatalf("reap on empty queue returned error: %v", err)
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()
	if len(ft.inReviewCalls) != 0 || len(ft.blockedCalls) != 0 {
		t.Error("unexpected transition calls on empty queue")
	}
}

// TestReap_BrokerError verifies that a non-200 broker response returns an error.
func TestReap_BrokerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	ft := &fakeTransition{}
	hs := NewHandleStore()
	r := newTestReaper(srv, ft)

	err := r.reap(context.Background(), nil, hs, uuid.New())
	if err == nil {
		t.Fatal("expected error for non-200 broker response, got nil")
	}
}

// TestReap_SnakeCaseMetadataDecode verifies that the broker's snake_case JSON
// metadata keys (feature_id, task_id) are decoded correctly into completionRecord.
// This tests the slow-path: metadata slugs are used to resolve the task when the
// handle is absent from the in-memory HandleStore (e.g. after an orchestrator restart).
func TestReap_SnakeCaseMetadataDecode(t *testing.T) {
	taskUUID := uuid.New()
	featureUUID := uuid.New()
	workspaceUUID := uuid.New()

	// The broker returns snake_case JSON — matching store.HandleMetadata tags.
	rawCompletion := `[{
		"handle": "handle-snake",
		"metadata": {
			"kind": "impl",
			"feature_id": "snake-feature",
			"task_id": "T42",
			"started_at": "2026-06-08T00:00:00Z"
		},
		"result": {
			"terminal_status": "in_review",
			"pr_url": "https://github.com/owner/repo/pull/10"
		}
	}]`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(rawCompletion))
	}))
	defer srv.Close()

	resolvedEntry := &HandleEntry{
		FeatureUUID: featureUUID,
		TaskUUID:    taskUUID,
		FeatureName: "snake-feature",
		TaskName:    "T42",
	}
	ft := &fakeTransition{slowLookupResult: resolvedEntry}
	hs := NewHandleStore() // empty — forces slow path using Metadata slugs

	r := newTestReaper(srv, ft)
	if err := r.reap(context.Background(), nil, hs, workspaceUUID); err != nil {
		t.Fatalf("reap error: %v", err)
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()

	// Verify the slow-path lookup was called with the snake_case-decoded slugs.
	if len(ft.inReviewCalls) != 1 {
		t.Fatalf("SetInReview called %d times, want 1", len(ft.inReviewCalls))
	}
	if ft.inReviewCalls[0].taskUUID != taskUUID {
		t.Errorf("taskUUID = %v, want %v", ft.inReviewCalls[0].taskUUID, taskUUID)
	}
	if ft.inReviewCalls[0].prURL != "https://github.com/owner/repo/pull/10" {
		t.Errorf("prURL = %q", ft.inReviewCalls[0].prURL)
	}
}

// TestReap_DLQ_Failed verifies that terminal_status "failed" calls SetBlocked
// with the provided blocked_reason (DLQ spawn-failure path).
func TestReap_DLQ_Failed(t *testing.T) {
	taskUUID := uuid.New()
	featureUUID := uuid.New()
	workspaceUUID := uuid.New()

	completion := completionRecord{
		Handle: "handle-dlq",
		Metadata: handleMetadata{
			FeatureID: "dlq-feat",
			TaskID:    "T9",
		},
		Result: executorResult{
			TerminalStatus: "failed",
			BlockedReason:  "spawn_dlq_failed",
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ackOK(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(makeCompletionResponse([]completionRecord{completion}))
	}))
	defer srv.Close()

	ft := &fakeTransition{}
	hs := NewHandleStore()
	hs.Register("handle-dlq", HandleEntry{
		FeatureUUID: featureUUID,
		TaskUUID:    taskUUID,
		FeatureName: "dlq-feat",
		TaskName:    "T9",
	})

	r := newTestReaper(srv, ft)
	if err := r.reap(context.Background(), nil, hs, workspaceUUID); err != nil {
		t.Fatalf("reap error: %v", err)
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()

	if len(ft.blockedCalls) != 1 {
		t.Fatalf("SetBlocked called %d times, want 1", len(ft.blockedCalls))
	}
	if ft.blockedCalls[0].taskUUID != taskUUID {
		t.Errorf("taskUUID = %v, want %v", ft.blockedCalls[0].taskUUID, taskUUID)
	}
	if ft.blockedCalls[0].reason != "spawn_dlq_failed" {
		t.Errorf("reason = %q, want spawn_dlq_failed", ft.blockedCalls[0].reason)
	}
	if len(ft.inReviewCalls) != 0 {
		t.Errorf("SetInReview should not be called for 'failed' terminal status")
	}
	if _, found := hs.Lookup("handle-dlq"); found {
		t.Error("handle should be deleted after processing")
	}
}

// TestReap_DLQ_Failed_DefaultReason verifies that an empty blocked_reason on a
// "failed" completion falls back to "spawn_dlq_failed".
func TestReap_DLQ_Failed_DefaultReason(t *testing.T) {
	taskUUID := uuid.New()
	featureUUID := uuid.New()
	workspaceUUID := uuid.New()

	completion := completionRecord{
		Handle: "handle-dlq-no-reason",
		Metadata: handleMetadata{
			FeatureID: "dlq-feat2",
			TaskID:    "T10",
		},
		Result: executorResult{
			TerminalStatus: "failed",
			BlockedReason:  "", // empty — should default
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ackOK(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(makeCompletionResponse([]completionRecord{completion}))
	}))
	defer srv.Close()

	ft := &fakeTransition{}
	hs := NewHandleStore()
	hs.Register("handle-dlq-no-reason", HandleEntry{
		FeatureUUID: featureUUID,
		TaskUUID:    taskUUID,
		FeatureName: "dlq-feat2",
		TaskName:    "T10",
	})

	r := newTestReaper(srv, ft)
	if err := r.reap(context.Background(), nil, hs, workspaceUUID); err != nil {
		t.Fatalf("reap error: %v", err)
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()

	if len(ft.blockedCalls) != 1 {
		t.Fatalf("SetBlocked called %d times, want 1", len(ft.blockedCalls))
	}
	if ft.blockedCalls[0].reason != "spawn_dlq_failed" {
		t.Errorf("default reason = %q, want spawn_dlq_failed", ft.blockedCalls[0].reason)
	}
}

// TestReap_MaxTurns_Retry verifies that terminal_status "max_turns" with a
// retry count below executorMaxRetries resets in_progress→ready.
func TestReap_MaxTurns_Retry(t *testing.T) {
	taskUUID := uuid.New()
	featureUUID := uuid.New()
	workspaceUUID := uuid.New()

	completion := completionRecord{
		Handle: "handle-mt-retry",
		Metadata: handleMetadata{
			FeatureID: "mt-feat",
			TaskID:    "T3",
		},
		Result: executorResult{
			TerminalStatus: "max_turns",
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ackOK(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(makeCompletionResponse([]completionRecord{completion}))
	}))
	defer srv.Close()

	// count=1 < executorMaxRetries=3 → should retry (set ready)
	ft := &fakeTransition{maxTurnsCount: 1}
	hs := NewHandleStore()
	hs.Register("handle-mt-retry", HandleEntry{
		FeatureUUID: featureUUID,
		TaskUUID:    taskUUID,
		FeatureName: "mt-feat",
		TaskName:    "T3",
	})

	r := newTestReaperWithRetries(srv, ft, 3)
	if err := r.reap(context.Background(), nil, hs, workspaceUUID); err != nil {
		t.Fatalf("reap error: %v", err)
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()

	if len(ft.readyMaxTurnsCalls) != 1 {
		t.Fatalf("SetReadyFromMaxTurns called %d times, want 1", len(ft.readyMaxTurnsCalls))
	}
	if ft.readyMaxTurnsCalls[0].taskUUID != taskUUID {
		t.Errorf("taskUUID = %v, want %v", ft.readyMaxTurnsCalls[0].taskUUID, taskUUID)
	}
	if len(ft.blockedCalls) != 0 {
		t.Errorf("SetBlocked should not be called when retries remain; called %d times", len(ft.blockedCalls))
	}
	if _, found := hs.Lookup("handle-mt-retry"); found {
		t.Error("handle should be deleted after processing")
	}
}

// TestReap_MaxTurns_Blocked verifies that terminal_status "max_turns" with a
// retry count at or above executorMaxRetries blocks the task.
func TestReap_MaxTurns_Blocked(t *testing.T) {
	taskUUID := uuid.New()
	featureUUID := uuid.New()
	workspaceUUID := uuid.New()

	completion := completionRecord{
		Handle: "handle-mt-block",
		Metadata: handleMetadata{
			FeatureID: "mt-feat2",
			TaskID:    "T4",
		},
		Result: executorResult{
			TerminalStatus: "max_turns",
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ackOK(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(makeCompletionResponse([]completionRecord{completion}))
	}))
	defer srv.Close()

	// count=3 >= executorMaxRetries=3 → should block
	ft := &fakeTransition{maxTurnsCount: 3}
	hs := NewHandleStore()
	hs.Register("handle-mt-block", HandleEntry{
		FeatureUUID: featureUUID,
		TaskUUID:    taskUUID,
		FeatureName: "mt-feat2",
		TaskName:    "T4",
	})

	r := newTestReaperWithRetries(srv, ft, 3)
	if err := r.reap(context.Background(), nil, hs, workspaceUUID); err != nil {
		t.Fatalf("reap error: %v", err)
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()

	if len(ft.blockedCalls) != 1 {
		t.Fatalf("SetBlocked called %d times, want 1", len(ft.blockedCalls))
	}
	if ft.blockedCalls[0].reason != "max_turns_exceeded" {
		t.Errorf("reason = %q, want max_turns_exceeded", ft.blockedCalls[0].reason)
	}
	if len(ft.readyMaxTurnsCalls) != 0 {
		t.Errorf("SetReadyFromMaxTurns should not be called when cap reached; called %d times", len(ft.readyMaxTurnsCalls))
	}
	if _, found := hs.Lookup("handle-mt-block"); found {
		t.Error("handle should be deleted after processing")
	}
}

// TestReap_MaxTurns_ZeroCount verifies that a max_turns event with count=0
// (first occurrence) still retries when executorMaxRetries > 0.
func TestReap_MaxTurns_ZeroCount(t *testing.T) {
	taskUUID := uuid.New()
	featureUUID := uuid.New()
	workspaceUUID := uuid.New()

	completion := completionRecord{
		Handle: "handle-mt-zero",
		Metadata: handleMetadata{
			FeatureID: "mt-feat3",
			TaskID:    "T5",
		},
		Result: executorResult{
			TerminalStatus: "max_turns",
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ackOK(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(makeCompletionResponse([]completionRecord{completion}))
	}))
	defer srv.Close()

	// count=0 < executorMaxRetries=3 → first occurrence; should retry
	ft := &fakeTransition{maxTurnsCount: 0}
	hs := NewHandleStore()
	hs.Register("handle-mt-zero", HandleEntry{
		FeatureUUID: featureUUID,
		TaskUUID:    taskUUID,
		FeatureName: "mt-feat3",
		TaskName:    "T5",
	})

	r := newTestReaperWithRetries(srv, ft, 3)
	if err := r.reap(context.Background(), nil, hs, workspaceUUID); err != nil {
		t.Fatalf("reap error: %v", err)
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()

	if len(ft.readyMaxTurnsCalls) != 1 {
		t.Fatalf("SetReadyFromMaxTurns called %d times, want 1", len(ft.readyMaxTurnsCalls))
	}
	if len(ft.blockedCalls) != 0 {
		t.Errorf("SetBlocked should not be called on first max_turns occurrence")
	}
}
