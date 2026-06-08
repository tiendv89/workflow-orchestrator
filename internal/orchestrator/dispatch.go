package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

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

// brokerRegisterRequest is the JSON body for POST /register.
type brokerRegisterRequest struct {
	Handle   string         `json:"handle"`
	Owner    string         `json:"owner"`
	Metadata handleMetadata `json:"metadata"`
}

type handleMetadata struct {
	FeatureID string `json:"FeatureID"`
	TaskID    string `json:"TaskID"`
	TenantID  string `json:"TenantID"`
	StartedAt string `json:"StartedAt"`
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
// The Dispatcher (and its underlying Streamer/Redis client) must be created once
// at startup and shared across calls to avoid connection leaks.
func (d *Dispatcher) Dispatch(ctx context.Context, cfg *config.Config, task db.WorkspaceTask, handle string) error {
	now := time.Now().UTC().Format(time.RFC3339)

	if err := d.registerHandle(ctx, handle, task, cfg.OrganizationID, now); err != nil {
		return fmt.Errorf("dispatch: broker register: %w", err)
	}

	if err := d.enqueueJob(ctx, cfg, task, handle); err != nil {
		return fmt.Errorf("dispatch: enqueue job: %w", err)
	}

	return nil
}

func (d *Dispatcher) registerHandle(ctx context.Context, handle string, task db.WorkspaceTask, tenantID, startedAt string) error {
	body := brokerRegisterRequest{
		Handle: handle,
		Owner:  "go",
		Metadata: handleMetadata{
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

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("broker returned status %d", resp.StatusCode)
	}
	return nil
}

func (d *Dispatcher) enqueueJob(ctx context.Context, cfg *config.Config, task db.WorkspaceTask, handle string) error {
	branch := ""
	if task.Branch != nil {
		branch = *task.Branch
	}

	values := map[string]string{
		"task_id":         task.TaskName,
		"feature_id":      task.FeatureName,
		"workspace_id":    cfg.WorkspaceID,
		"handle":          handle,
		"management_repo": cfg.ManagementRepo,
		"base_branch":     cfg.BaseBranch,
		"branch":          branch,
	}

	return d.stream.StreamAdd(ctx, "platform:dispatch", values)
}
