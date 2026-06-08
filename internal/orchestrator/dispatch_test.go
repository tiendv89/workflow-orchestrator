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
	}
}

// TestDispatch_BrokerRegistration verifies that dispatch POSTs the correct JSON
// body to /register and returns nil when the broker responds 200.
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
	d := NewDispatcher(srv.URL, ms, srv.Client())
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
}

// TestDispatch_RedisStreamEntry verifies that dispatch calls StreamAdd with the
// correct stream name and ABI fields.
func TestDispatch_RedisStreamEntry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ms := &mockStreamer{}
	d := NewDispatcher(srv.URL, ms, srv.Client())
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

	check := func(key, want string) {
		t.Helper()
		if got := call.values[key]; got != want {
			t.Errorf("values[%q] = %q, want %q", key, got, want)
		}
	}
	check("task_id", "T11")
	check("feature_id", "my-feature")
	check("workspace_id", "ws-123")
	check("handle", "handle-xyz")
	check("management_repo", "owner/repo")
	check("base_branch", "main")
	check("branch", "feature/my-feature-T11")
}

// TestDispatch_BrokerError verifies that a non-200 broker response returns an error.
func TestDispatch_BrokerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	ms := &mockStreamer{}
	d := NewDispatcher(srv.URL, ms, srv.Client())
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
	d := NewDispatcher(srv.URL, ms, srv.Client())
	cfg := newTestConfig(srv.URL)
	task := newTestTask()

	err := d.Dispatch(context.Background(), cfg, task, "handle-redis-err")
	if err == nil {
		t.Fatal("expected error for redis failure, got nil")
	}
}
