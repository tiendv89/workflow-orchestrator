package orchestrator

// reviewer_test.go covers the reap verdict routing and DispatchWithNonce
// behaviour added by T7 (reviewer cluster). All tests here stay in the
// internal package so they can access unexported types (fakeTransition,
// brokerRegisterRequest, dispatchJob, etc.) defined in the same package.
//
// DB integration tests for HandleNoVerdict and ClaimFix live in
// reviewer_integration_test.go (package orchestrator_test).

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	db "github.com/tiendv89/workflow-orchestrator/internal/database/queries"
	gh "github.com/tiendv89/workflow-orchestrator/internal/github"
)

// --- Reap verdict routing tests ---

// buildReviewCompletion builds a completionRecord with kind="review".
func buildReviewCompletion(handle, featureID, taskID, terminalStatus string) completionRecord {
	return completionRecord{
		Handle: handle,
		Metadata: handleMetadata{
			Kind:      "review",
			FeatureID: featureID,
			TaskID:    taskID,
		},
		Result: executorResult{
			TerminalStatus: terminalStatus,
		},
	}
}

// TestReap_ReviewPassed verifies that a "review_passed" completion from a
// reviewer calls SetReviewPassed (APPROVE verdict).
func TestReap_ReviewPassed(t *testing.T) {
	taskUUID := uuid.New()
	featureUUID := uuid.New()
	workspaceUUID := uuid.New()

	completion := buildReviewCompletion("handle-rp", "feat", "T1", "review_passed")

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
	hs.Register("handle-rp", HandleEntry{
		FeatureUUID: featureUUID,
		TaskUUID:    taskUUID,
		FeatureName: "feat",
		TaskName:    "T1",
	})

	r := newTestReaper(srv, ft)
	if err := r.reap(context.Background(), nil, hs, workspaceUUID); err != nil {
		t.Fatalf("reap error: %v", err)
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()
	if len(ft.reviewPassedCalls) != 1 {
		t.Fatalf("SetReviewPassed called %d times, want 1", len(ft.reviewPassedCalls))
	}
	if ft.reviewPassedCalls[0].taskUUID != taskUUID {
		t.Errorf("taskUUID = %v, want %v", ft.reviewPassedCalls[0].taskUUID, taskUUID)
	}
	if len(ft.changeRequestedCalls) != 0 || len(ft.noVerdictCalls) != 0 {
		t.Error("unexpected change_requested or no-verdict calls")
	}
	if _, found := hs.Lookup("handle-rp"); found {
		t.Error("handle still in store after reap")
	}
}

// TestReap_ChangeRequested verifies that a "change_requested" completion from a
// reviewer calls SetChangeRequested (REQUEST_CHANGES verdict).
func TestReap_ChangeRequested(t *testing.T) {
	taskUUID := uuid.New()
	featureUUID := uuid.New()
	workspaceUUID := uuid.New()

	completion := buildReviewCompletion("handle-cr", "feat", "T2", "change_requested")

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
	hs.Register("handle-cr", HandleEntry{
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
	if len(ft.changeRequestedCalls) != 1 {
		t.Fatalf("SetChangeRequested called %d times, want 1", len(ft.changeRequestedCalls))
	}
	if ft.changeRequestedCalls[0].taskUUID != taskUUID {
		t.Errorf("taskUUID = %v, want %v", ft.changeRequestedCalls[0].taskUUID, taskUUID)
	}
	if len(ft.reviewPassedCalls) != 0 || len(ft.noVerdictCalls) != 0 {
		t.Error("unexpected review_passed or no-verdict calls")
	}
}

// TestReap_ReviewBlocked_NoVerdict verifies that a "blocked" completion from a
// reviewer (kind=review) calls HandleNoVerdict, not SetBlocked.
func TestReap_ReviewBlocked_NoVerdict(t *testing.T) {
	taskUUID := uuid.New()
	featureUUID := uuid.New()
	workspaceUUID := uuid.New()

	completion := completionRecord{
		Handle: "handle-rb",
		Metadata: handleMetadata{
			Kind:      "review",
			FeatureID: "feat",
			TaskID:    "T3",
		},
		Result: executorResult{
			TerminalStatus: "blocked",
			BlockedReason:  "reviewer_timed_out",
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
	hs.Register("handle-rb", HandleEntry{
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
	if len(ft.noVerdictCalls) != 1 {
		t.Fatalf("HandleNoVerdict called %d times, want 1", len(ft.noVerdictCalls))
	}
	if ft.noVerdictCalls[0].taskUUID != taskUUID {
		t.Errorf("taskUUID = %v, want %v", ft.noVerdictCalls[0].taskUUID, taskUUID)
	}
	if ft.noVerdictCalls[0].max != MaxReviewIncompletes {
		t.Errorf("max = %d, want %d", ft.noVerdictCalls[0].max, MaxReviewIncompletes)
	}
	if len(ft.blockedCalls) != 0 {
		t.Error("SetBlocked must not be called for review-kind blocked result")
	}
}

// TestReap_ImplBlocked_SetsBlocked verifies that a "blocked" completion from an
// impl task (kind="impl") calls SetBlocked, not HandleNoVerdict.
func TestReap_ImplBlocked_SetsBlocked(t *testing.T) {
	taskUUID := uuid.New()
	featureUUID := uuid.New()
	workspaceUUID := uuid.New()

	completion := completionRecord{
		Handle: "handle-ib",
		Metadata: handleMetadata{
			Kind:      "impl",
			FeatureID: "feat",
			TaskID:    "T4",
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
	hs.Register("handle-ib", HandleEntry{
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
	if ft.blockedCalls[0].reason != "tests_failed" {
		t.Errorf("reason = %q, want tests_failed", ft.blockedCalls[0].reason)
	}
	if len(ft.noVerdictCalls) != 0 {
		t.Error("HandleNoVerdict must not be called for impl-kind blocked result")
	}
}

// TestReap_ReviewUnknownStatus_NoVerdict verifies that an unrecognised
// terminal_status from a reviewer (kind=review) triggers HandleNoVerdict.
func TestReap_ReviewUnknownStatus_NoVerdict(t *testing.T) {
	taskUUID := uuid.New()
	featureUUID := uuid.New()
	workspaceUUID := uuid.New()

	completion := completionRecord{
		Handle: "handle-ru",
		Metadata: handleMetadata{
			Kind:      "review",
			FeatureID: "feat",
			TaskID:    "T5",
		},
		Result: executorResult{
			TerminalStatus: "something_unexpected",
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
	hs.Register("handle-ru", HandleEntry{
		FeatureUUID: featureUUID,
		TaskUUID:    taskUUID,
		FeatureName: "feat",
		TaskName:    "T5",
	})

	r := newTestReaper(srv, ft)
	if err := r.reap(context.Background(), nil, hs, workspaceUUID); err != nil {
		t.Fatalf("reap error: %v", err)
	}

	ft.mu.Lock()
	defer ft.mu.Unlock()
	if len(ft.noVerdictCalls) != 1 {
		t.Fatalf("HandleNoVerdict called %d times, want 1", len(ft.noVerdictCalls))
	}
}

// TestReap_FixBlocked_SetsBlocked verifies that a "blocked" completion from a
// fix agent (kind="fix") calls SetBlocked, not HandleNoVerdict.
func TestReap_FixBlocked_SetsBlocked(t *testing.T) {
	taskUUID := uuid.New()
	featureUUID := uuid.New()
	workspaceUUID := uuid.New()

	completion := completionRecord{
		Handle: "handle-fb",
		Metadata: handleMetadata{
			Kind:      "fix",
			FeatureID: "feat",
			TaskID:    "T6",
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
	hs.Register("handle-fb", HandleEntry{
		FeatureUUID: featureUUID,
		TaskUUID:    taskUUID,
		FeatureName: "feat",
		TaskName:    "T6",
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
	if ft.blockedCalls[0].reason != "missing_tool" {
		t.Errorf("reason = %q, want missing_tool", ft.blockedCalls[0].reason)
	}
	if len(ft.noVerdictCalls) != 0 {
		t.Error("HandleNoVerdict must not be called for fix-kind blocked result")
	}
}

// --- DispatchWithNonce tests ---

// jsonBodyCapture reads a POST body and unmarshals it into dst.
func jsonBodyCapture(r *http.Request, dst any) {
	b, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(b, dst)
}

// TestDispatchWithNonce_KindReview verifies that DispatchWithNonce passes
// kind="review" to both /register and the Redis stream job.
func TestDispatchWithNonce_KindReview(t *testing.T) {
	var capturedKind string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/register" {
			var body brokerRegisterRequest
			jsonBodyCapture(r, &body)
			capturedKind = body.Metadata.Kind
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ms := &mockStreamer{}
	d := newTestDispatcher(srv.URL, ms, srv.Client())
	cfg := newTestConfig(srv.URL)
	task := newTestTask()

	if err := d.DispatchWithNonce(context.Background(), cfg, task, "handle-rv", "nonce-rv", "review"); err != nil {
		t.Fatalf("DispatchWithNonce error: %v", err)
	}

	if capturedKind != "review" {
		t.Errorf("broker kind = %q, want review", capturedKind)
	}

	if len(ms.calls) != 1 {
		t.Fatalf("StreamAdd called %d times, want 1", len(ms.calls))
	}
	var job dispatchJob
	if err := json.Unmarshal([]byte(ms.calls[0].values["job"]), &job); err != nil {
		t.Fatalf("unmarshal job: %v", err)
	}
	if job.Kind != "review" {
		t.Errorf("job.Kind = %q, want review", job.Kind)
	}
}

// TestDispatchWithNonce_KindFix verifies kind="fix" round-trips correctly.
func TestDispatchWithNonce_KindFix(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ms := &mockStreamer{}
	d := newTestDispatcher(srv.URL, ms, srv.Client())

	if err := d.DispatchWithNonce(context.Background(), newTestConfig(srv.URL), newTestTask(), "h-fix", "n-fix", "fix"); err != nil {
		t.Fatalf("DispatchWithNonce error: %v", err)
	}

	if len(ms.calls) != 1 {
		t.Fatalf("StreamAdd called %d times, want 1", len(ms.calls))
	}
	var job dispatchJob
	if err := json.Unmarshal([]byte(ms.calls[0].values["job"]), &job); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if job.Kind != "fix" {
		t.Errorf("job.Kind = %q, want fix", job.Kind)
	}
}

// TestDispatchWithNonce_NoncePreserved verifies that the externally-supplied
// nonce is used unchanged in both /register and the stream job.
func TestDispatchWithNonce_NoncePreserved(t *testing.T) {
	externalNonce := "my-predetermined-nonce-" + uuid.New().String()
	var registeredNonce string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/register" {
			var body brokerRegisterRequest
			jsonBodyCapture(r, &body)
			registeredNonce = body.Nonce
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ms := &mockStreamer{}
	d := newTestDispatcher(srv.URL, ms, srv.Client())
	_ = d.DispatchWithNonce(context.Background(), newTestConfig(srv.URL), newTestTask(), "h", externalNonce, "review")

	if registeredNonce != externalNonce {
		t.Errorf("broker nonce = %q, want %q", registeredNonce, externalNonce)
	}

	if len(ms.calls) == 0 {
		t.Fatal("StreamAdd not called")
	}
	var job dispatchJob
	_ = json.Unmarshal([]byte(ms.calls[0].values["job"]), &job)
	if job.Nonce != externalNonce {
		t.Errorf("job.Nonce = %q, want %q", job.Nonce, externalNonce)
	}
}

// TestDispatch_DefaultKindIsImpl verifies that the existing Dispatch() still
// produces kind="impl" (regression guard for the DispatchWithNonce refactor).
func TestDispatch_DefaultKindIsImpl(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ms := &mockStreamer{}
	d := newTestDispatcher(srv.URL, ms, srv.Client())

	if err := d.Dispatch(context.Background(), newTestConfig(srv.URL), newTestTask(), "h-impl"); err != nil {
		t.Fatalf("Dispatch error: %v", err)
	}

	if len(ms.calls) == 0 {
		t.Fatal("StreamAdd not called")
	}
	var job dispatchJob
	if err := json.Unmarshal([]byte(ms.calls[0].values["job"]), &job); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if job.Kind != "impl" {
		t.Errorf("Dispatch() job.Kind = %q, want impl (regression)", job.Kind)
	}
}

// --- DispatchReviewer merged-PR skip tests ---

// fakePRGetter is a lightweight gh.PRGetter stub for unit tests.
type fakePRGetter struct {
	merged bool
	err    error
}

func (f *fakePRGetter) GetPR(_ context.Context, _ string) (*gh.PRStatus, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &gh.PRStatus{Merged: f.merged}, nil
}

// prTaskWithURL returns a minimal WorkspaceTask with a PR URL in the Pr field.
func prTaskWithURL(prURL string) db.WorkspaceTask {
	return db.WorkspaceTask{
		FeatureName: "feat",
		TaskName:    "T1",
		Pr:          []byte(`{"url":"` + prURL + `","status":"open"}`),
	}
}

// TestDispatchReviewer_MergedPR_Skips verifies that DispatchReviewer returns
// (false, nil) without touching the DB when the ghClient reports the PR merged.
func TestDispatchReviewer_MergedPR_Skips(t *testing.T) {
	task := prTaskWithURL("https://github.com/o/r/pull/1")
	ghClient := &fakePRGetter{merged: true}

	// pool is nil — if the function touches it, it will panic, failing the test.
	won, err := DispatchReviewer(context.Background(), nil, nil, uuid.New(), task, nil, nil, ghClient)
	if err != nil {
		t.Fatalf("DispatchReviewer error: %v", err)
	}
	if won {
		t.Error("DispatchReviewer: expected won=false for merged PR, got true")
	}
}

// TestDispatchReviewer_NotMergedPR_DoesNotSkip verifies that when the PR is
// not merged, DispatchReviewer proceeds past the merged check and attempts
// SetReviewing (as evidenced by the nil pool panic being recovered here).
func TestDispatchReviewer_NotMergedPR_DoesNotSkip(t *testing.T) {
	task := prTaskWithURL("https://github.com/o/r/pull/4")
	ghClient := &fakePRGetter{merged: false}

	// With merged=false the function should NOT short-circuit; it will proceed
	// to SetReviewing and panic on the nil pool. Recover the panic — that is the
	// confirmation that the code DID proceed past the merged check.
	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		//nolint:errcheck
		DispatchReviewer(context.Background(), nil, nil, uuid.New(), task, nil, nil, ghClient)
	}()

	if !panicked {
		t.Error("expected nil-pool panic (proof that merged check did not short-circuit), got clean return")
	}
}
