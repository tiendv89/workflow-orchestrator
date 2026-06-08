package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/tiendv89/workflow-orchestrator/internal/config"
	db "github.com/tiendv89/workflow-orchestrator/internal/database/queries"
)

// Streamer is the minimal interface for appending to a Redis stream.
// The real implementation wraps *redis.Client; tests inject a mock.
type Streamer interface {
	StreamAdd(ctx context.Context, stream string, values map[string]string) error
}

// redisStreamer adapts *redis.Client to Streamer.
type redisStreamer struct {
	rdb *redis.Client
}

// NewRedisStreamer wraps a pre-created *redis.Client as a Streamer.
// Create the client once at startup and reuse across all dispatch calls.
func NewRedisStreamer(rdb *redis.Client) Streamer {
	return &redisStreamer{rdb: rdb}
}

func (r *redisStreamer) StreamAdd(ctx context.Context, stream string, values map[string]string) error {
	args := &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}
	return r.rdb.XAdd(ctx, args).Err()
}

// handleMetadata represents the broker's HandleMetadata.
// JSON tags are snake_case to match agent-workflow/runtime/broker/internal/store/store.go:18-23.
type handleMetadata struct {
	Kind      string `json:"kind"`
	TaskID    string `json:"task_id"`
	FeatureID string `json:"feature_id"`
	TenantID  string `json:"tenant_id,omitempty"`
	StartedAt string `json:"started_at"`
}

// brokerRegisterRequest is the JSON body for POST /register.
type brokerRegisterRequest struct {
	Handle   string         `json:"handle"`
	Nonce    string         `json:"nonce"`
	Owner    string         `json:"owner,omitempty"`
	Metadata handleMetadata `json:"metadata"`
}

// dispatchJob is the payload marshalled as the single "job" field on the Redis dispatch stream.
// It mirrors DispatchJob in agent-workflow/runtime/abi/src/types.ts:50-96.
type dispatchJob struct {
	Handle             string `json:"handle"`
	Nonce              string `json:"nonce"`
	Kind               string `json:"kind"`
	TaskID             string `json:"task_id"`
	FeatureID          string `json:"feature_id"`
	WorkspaceID        string `json:"workspace_id"`
	TaskRepoURL        string `json:"task_repo_url"`
	TaskRepoBranch     string `json:"task_repo_branch"`
	TaskBaseBranch     string `json:"task_base_branch"`
	TaskRepoBaseBranch string `json:"task_repo_base_branch"`
	MgmtRepoURL        string `json:"mgmt_repo_url"`
	CallbackURL        string `json:"callback_url"`
	EnqueuedAt         string `json:"enqueued_at"`
}

// Dispatcher handles broker registration and Redis stream enqueuing for a task.
// Create one instance at startup via NewDispatcher and reuse across the poll loop.
type Dispatcher struct {
	brokerURL  string
	httpClient *http.Client
	stream     Streamer
}

// NewDispatcher constructs a Dispatcher. Pass nil for httpClient to use the default.
func NewDispatcher(brokerURL string, stream Streamer, httpClient *http.Client) *Dispatcher {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &Dispatcher{brokerURL: brokerURL, httpClient: httpClient, stream: stream}
}

// Dispatch registers the handle with the broker and enqueues the DispatchJob.
// A single-use nonce is generated here and threaded through both the /register
// call and the stream entry so the executor's /callback validates correctly.
func (d *Dispatcher) Dispatch(ctx context.Context, cfg *config.Config, task db.WorkspaceTask, handle string) error {
	nonce := uuid.New().String()
	now := time.Now().UTC().Format(time.RFC3339)

	if err := d.registerHandle(ctx, handle, task, cfg.OrganizationID, nonce, now); err != nil {
		return fmt.Errorf("dispatch: broker register: %w", err)
	}

	if err := d.enqueueJob(ctx, cfg, task, handle, nonce, now); err != nil {
		return fmt.Errorf("dispatch: enqueue job: %w", err)
	}

	return nil
}

func (d *Dispatcher) registerHandle(ctx context.Context, handle string, task db.WorkspaceTask, tenantID, nonce, startedAt string) error {
	body := brokerRegisterRequest{
		Handle: handle,
		Nonce:  nonce,
		Owner:  "go",
		Metadata: handleMetadata{
			Kind:      "impl",
			FeatureID: task.FeatureName,
			TaskID:    task.TaskName,
			TenantID:  tenantID,
			StartedAt: startedAt,
		},
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.brokerURL+"/register", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK &&
		resp.StatusCode != http.StatusNoContent &&
		resp.StatusCode != http.StatusConflict {
		return fmt.Errorf("broker register: unexpected status %d", resp.StatusCode)
	}
	return nil
}

func (d *Dispatcher) enqueueJob(ctx context.Context, cfg *config.Config, task db.WorkspaceTask, handle, nonce, now string) error {
	branch := ""
	if task.Branch != nil {
		branch = *task.Branch
	}

	job := dispatchJob{
		Handle:             handle,
		Nonce:              nonce,
		Kind:               "impl",
		TaskID:             task.TaskName,
		FeatureID:          task.FeatureName,
		WorkspaceID:        cfg.WorkspaceID,
		TaskRepoURL:        cfg.ImplRepoURL,
		TaskRepoBranch:     branch,
		TaskBaseBranch:     cfg.BaseBranch,
		TaskRepoBaseBranch: cfg.BaseBranch,
		MgmtRepoURL:        cfg.ManagementRepo,
		CallbackURL:        cfg.BrokerURL + "/callback",
		EnqueuedAt:         now,
	}

	payload, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal dispatch job: %w", err)
	}

	return d.stream.StreamAdd(ctx, "platform:dispatch", map[string]string{
		"job": string(payload),
	})
}
