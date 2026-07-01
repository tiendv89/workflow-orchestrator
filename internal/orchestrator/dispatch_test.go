package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/tiendv89/workflow-orchestrator/internal/config"
	db "github.com/tiendv89/workflow-orchestrator/internal/database/queries"
)

// mockDBQuerier is a test double for dbQuerier.
type mockDBQuerier struct {
	modelID  string
	modelErr error
}

func (m *mockDBQuerier) GetWorkspaceRepo(_ context.Context, _ db.GetWorkspaceRepoParams) (db.WorkspaceRepo, error) {
	return db.WorkspaceRepo{RepoURL: strPtr("git@github.com:owner/impl-repo.git")}, nil
}

func (m *mockDBQuerier) GetWorkspaceDefaultModel(_ context.Context, _ db.GetWorkspaceDefaultModelParams) (db.Model, error) {
	return db.Model{ModelID: m.modelID}, m.modelErr
}

// mockStreamer captures StreamAdd calls for assertion.
type mockStreamer struct {
	mu    sync.Mutex
	calls []streamCall
	err   error
}

type streamCall struct {
	stream string
	values map[string]string
}

func (m *mockStreamer) StreamAdd(_ context.Context, stream string, values map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// copy values to avoid mutation after the call
	cp := make(map[string]string, len(values))
	for k, v := range values {
		cp[k] = v
	}
	m.calls = append(m.calls, streamCall{stream: stream, values: cp})
	return m.err
}

func strPtr(s string) *string { return &s }

func newTestConfig(brokerURL string) *config.Config {
	return &config.Config{
		WorkspaceID:    "ws-123",
		OrganizationID: "org-456",
		BrokerURL:      brokerURL,
		ManagementRepo: "owner/repo",
		BaseBranch:     "main",
	}
}

func newTestTask() db.WorkspaceTask {
	return db.WorkspaceTask{
		FeatureName: "my-feature",
		TaskName:    "T11",
		Branch:      strPtr("feature/my-feature-T11"),
		Repo:        strPtr("impl-repo"),
	}
}

// newTestDispatcher creates a Dispatcher wired with a default mockDBQuerier.
func newTestDispatcher(brokerURL string, stream Streamer, client *http.Client) *Dispatcher {
	d := NewDispatcher(brokerURL, stream, client, nil)
	d.db = &mockDBQuerier{}
	return d
}

// TestDispatch_BrokerRegistration verifies that dispatch POSTs the correct JSON
// body to /register and returns nil when the broker responds 200.
// Asserts snake_case metadata keys, nonce presence, and owner="go".
func TestDispatch_BrokerRegistration(t *testing.T) {
	var capturedBody brokerRegisterRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/register" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("unexpected method %q", r.Method)
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &capturedBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ms := &mockStreamer{}
	d := newTestDispatcher(srv.URL, ms, srv.Client())
	cfg := newTestConfig(srv.URL)
	task := newTestTask()

	if err := d.Dispatch(context.Background(), cfg, task, "handle-abc"); err != nil {
		t.Fatalf("dispatch returned error: %v", err)
	}

	if capturedBody.Handle != "handle-abc" {
		t.Errorf("handle = %q, want handle-abc", capturedBody.Handle)
	}
	if capturedBody.Owner != "go" {
		t.Errorf("owner = %q, want go", capturedBody.Owner)
	}
	if capturedBody.Nonce == "" {
		t.Error("nonce must not be empty")
	}
	if capturedBody.Metadata.FeatureID != "my-feature" {
		t.Errorf("FeatureID = %q, want my-feature", capturedBody.Metadata.FeatureID)
	}
	if capturedBody.Metadata.TaskID != "T11" {
		t.Errorf("TaskID = %q, want T11", capturedBody.Metadata.TaskID)
	}
	if capturedBody.Metadata.TenantID != "org-456" {
		t.Errorf("TenantID = %q, want org-456", capturedBody.Metadata.TenantID)
	}
	if capturedBody.Metadata.StartedAt == "" {
		t.Error("StartedAt must not be empty")
	}
	if capturedBody.Metadata.Kind != "impl" {
		t.Errorf("Kind = %q, want impl", capturedBody.Metadata.Kind)
	}
}

// TestDispatch_NonceConsistency verifies that the nonce on /register matches
// the nonce embedded in the dispatched DispatchJob on the stream.
func TestDispatch_NonceConsistency(t *testing.T) {
	var registeredNonce string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/register" {
			var body brokerRegisterRequest
			body2, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body2, &body)
			registeredNonce = body.Nonce
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ms := &mockStreamer{}
	d := newTestDispatcher(srv.URL, ms, srv.Client())
	cfg := newTestConfig(srv.URL)
	task := newTestTask()

	if err := d.Dispatch(context.Background(), cfg, task, "handle-nonce-check"); err != nil {
		t.Fatalf("dispatch returned error: %v", err)
	}

	if registeredNonce == "" {
		t.Fatal("no nonce captured from /register call")
	}

	if len(ms.calls) != 1 {
		t.Fatalf("StreamAdd called %d times, want 1", len(ms.calls))
	}

	jobJSON := ms.calls[0].values["job"]
	if jobJSON == "" {
		t.Fatal("stream entry missing 'job' field")
	}
	var job dispatchJob
	if err := json.Unmarshal([]byte(jobJSON), &job); err != nil {
		t.Fatalf("parse job JSON: %v", err)
	}
	if job.Nonce != registeredNonce {
		t.Errorf("job.Nonce = %q, want %q (registered nonce)", job.Nonce, registeredNonce)
	}
}

// TestDispatch_StreamEntry verifies that dispatch calls StreamAdd with a single
// "job" field containing a parseable DispatchJob with all required ABI fields.
func TestDispatch_StreamEntry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ms := &mockStreamer{}
	d := newTestDispatcher(srv.URL, ms, srv.Client())
	cfg := newTestConfig(srv.URL)
	task := newTestTask()

	if err := d.Dispatch(context.Background(), cfg, task, "handle-xyz"); err != nil {
		t.Fatalf("dispatch returned error: %v", err)
	}

	if len(ms.calls) != 1 {
		t.Fatalf("StreamAdd called %d times, want 1", len(ms.calls))
	}
	call := ms.calls[0]
	if call.stream != "platform:dispatch" {
		t.Errorf("stream = %q, want platform:dispatch", call.stream)
	}

	// The stream entry must have exactly one field: "job".
	jobJSON, ok := call.values["job"]
	if !ok || jobJSON == "" {
		t.Fatalf("stream entry missing 'job' field; got fields: %v", call.values)
	}

	// The "job" field must parse as a valid DispatchJob.
	var job dispatchJob
	if err := json.Unmarshal([]byte(jobJSON), &job); err != nil {
		t.Fatalf("'job' field is not valid JSON: %v", err)
	}

	// Verify all required ABI fields.
	if job.Handle != "handle-xyz" {
		t.Errorf("job.Handle = %q, want handle-xyz", job.Handle)
	}
	if job.Nonce == "" {
		t.Error("job.Nonce must not be empty")
	}
	if job.Kind != "impl" {
		t.Errorf("job.Kind = %q, want impl", job.Kind)
	}
	if job.TaskID != "T11" {
		t.Errorf("job.TaskID = %q, want T11", job.TaskID)
	}
	if job.FeatureID != "my-feature" {
		t.Errorf("job.FeatureID = %q, want my-feature", job.FeatureID)
	}
	if job.WorkspaceID != "ws-123" {
		t.Errorf("job.WorkspaceID = %q, want ws-123", job.WorkspaceID)
	}
	if job.TaskRepoURL != "git@github.com:owner/impl-repo.git" {
		t.Errorf("job.TaskRepoURL = %q, want git@github.com:owner/impl-repo.git", job.TaskRepoURL)
	}
	if job.TaskRepoBranch != "feature/my-feature-T11" {
		t.Errorf("job.TaskRepoBranch = %q, want feature/my-feature-T11", job.TaskRepoBranch)
	}
	if job.MgmtRepoURL != "owner/repo" {
		t.Errorf("job.MgmtRepoURL = %q, want owner/repo", job.MgmtRepoURL)
	}
	if job.CallbackURL != srv.URL+"/callback" {
		t.Errorf("job.CallbackURL = %q, want %s/callback", job.CallbackURL, srv.URL)
	}
	if job.EnqueuedAt == "" {
		t.Error("job.EnqueuedAt must not be empty")
	}
}

// TestDispatch_MetadataSnakeCaseRoundTrip verifies that handleMetadata JSON
// tags are snake_case, so broker decoding of "feature_id"/"task_id" works.
func TestDispatch_MetadataSnakeCaseRoundTrip(t *testing.T) {
	meta := handleMetadata{
		Kind:      "impl",
		FeatureID: "my-feature",
		TaskID:    "T11",
		TenantID:  "tenant-1",
		StartedAt: "2026-06-08T00:00:00Z",
	}

	b, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Verify the marshalled JSON uses snake_case keys.
	var raw map[string]string
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatalf("unmarshal to map: %v", err)
	}
	for _, key := range []string{"kind", "feature_id", "task_id", "tenant_id", "started_at"} {
		if _, ok := raw[key]; !ok {
			t.Errorf("JSON key %q missing; keys present: %v", key, raw)
		}
	}

	// Round-trip: decode back into a struct.
	var decoded handleMetadata
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("round-trip unmarshal: %v", err)
	}
	if decoded.FeatureID != meta.FeatureID {
		t.Errorf("FeatureID round-trip: got %q, want %q", decoded.FeatureID, meta.FeatureID)
	}
	if decoded.TaskID != meta.TaskID {
		t.Errorf("TaskID round-trip: got %q, want %q", decoded.TaskID, meta.TaskID)
	}
	if decoded.Kind != meta.Kind {
		t.Errorf("Kind round-trip: got %q, want %q", decoded.Kind, meta.Kind)
	}
}

// TestDispatch_BrokerError verifies that a non-200 broker response returns an error.
func TestDispatch_BrokerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	ms := &mockStreamer{}
	d := newTestDispatcher(srv.URL, ms, srv.Client())
	cfg := newTestConfig(srv.URL)
	task := newTestTask()

	err := d.Dispatch(context.Background(), cfg, task, "handle-err")
	if err == nil {
		t.Fatal("expected error for non-200 broker response, got nil")
	}
}

// TestDispatch_StreamError verifies that a Redis error is propagated.
func TestDispatch_StreamError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ms := &mockStreamer{err: errors.New("redis: connection refused")}
	d := newTestDispatcher(srv.URL, ms, srv.Client())
	cfg := newTestConfig(srv.URL)
	task := newTestTask()

	err := d.Dispatch(context.Background(), cfg, task, "handle-redis-err")
	if err == nil {
		t.Fatal("expected error for redis failure, got nil")
	}
}

// TestDispatch_TaskBaseBranch verifies that task_base_branch is set to
// "feature/<featureName>" (the feature-level PR target), not the repo base branch.
func TestDispatch_TaskBaseBranch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ms := &mockStreamer{}
	d := newTestDispatcher(srv.URL, ms, srv.Client())
	cfg := newTestConfig(srv.URL)
	task := newTestTask()

	if err := d.Dispatch(context.Background(), cfg, task, "handle-base-branch"); err != nil {
		t.Fatalf("dispatch returned error: %v", err)
	}

	var job dispatchJob
	if err := json.Unmarshal([]byte(ms.calls[0].values["job"]), &job); err != nil {
		t.Fatalf("unmarshal job: %v", err)
	}
	if job.TaskBaseBranch != "feature/my-feature" {
		t.Errorf("job.TaskBaseBranch = %q, want feature/my-feature", job.TaskBaseBranch)
	}
}

// TestDispatch_DerivesTaskRepoBranchWhenMissing verifies dispatch still sets
// task_repo_branch when the task row has no persisted branch (the common case
// before claim updates the DB, or when dispatch runs on a stale in-memory task).
func TestDispatch_DerivesTaskRepoBranchWhenMissing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ms := &mockStreamer{}
	d := newTestDispatcher(srv.URL, ms, srv.Client())
	cfg := newTestConfig(srv.URL)
	task := db.WorkspaceTask{
		FeatureName: "my-feature",
		TaskName:    "T11",
		Repo:        strPtr("impl-repo"),
	}

	if err := d.Dispatch(context.Background(), cfg, task, "handle-branch"); err != nil {
		t.Fatalf("dispatch returned error: %v", err)
	}

	var job dispatchJob
	if err := json.Unmarshal([]byte(ms.calls[0].values["job"]), &job); err != nil {
		t.Fatalf("unmarshal job: %v", err)
	}
	if job.TaskRepoBranch != "feature/my-feature-T11" {
		t.Errorf("job.TaskRepoBranch = %q, want feature/my-feature-T11", job.TaskRepoBranch)
	}
}

// TestDispatch_ModelFromPolicy verifies that the dispatched job contains the model
// returned by the workspace policy when a DB querier is configured.
func TestDispatch_ModelFromPolicy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ms := &mockStreamer{}
	d := newTestDispatcher(srv.URL, ms, srv.Client())
	d.db = &mockDBQuerier{modelID: "claude-opus-4-8"}
	cfg := newTestConfig(srv.URL)

	if err := d.Dispatch(context.Background(), cfg, newTestTask(), "handle-model-policy"); err != nil {
		t.Fatalf("dispatch returned error: %v", err)
	}

	var job dispatchJob
	if err := json.Unmarshal([]byte(ms.calls[0].values["job"]), &job); err != nil {
		t.Fatalf("unmarshal job: %v", err)
	}
	if job.ImplementationModel != "claude-opus-4-8" {
		t.Errorf("ImplementationModel = %q, want claude-opus-4-8", job.ImplementationModel)
	}
}

// TestDispatch_ModelFallbackOnDBError verifies that the dispatched job falls back
// to the default model when the DB query returns an error.
func TestDispatch_ModelFallbackOnDBError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ms := &mockStreamer{}
	d := newTestDispatcher(srv.URL, ms, srv.Client())
	d.db = &mockDBQuerier{modelErr: errors.New("no rows")}
	cfg := newTestConfig(srv.URL)

	if err := d.Dispatch(context.Background(), cfg, newTestTask(), "handle-model-err"); err != nil {
		t.Fatalf("dispatch returned error: %v", err)
	}

	var job dispatchJob
	if err := json.Unmarshal([]byte(ms.calls[0].values["job"]), &job); err != nil {
		t.Fatalf("unmarshal job: %v", err)
	}
	if job.ImplementationModel != defaultImplementationModel {
		t.Errorf("ImplementationModel = %q, want %q", job.ImplementationModel, defaultImplementationModel)
	}
}
