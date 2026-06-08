package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/tiendv89/workflow-orchestrator/internal/config"
)

// completionRecord is one entry returned by the broker's /list-completed endpoint.
type completionRecord struct {
	Handle   string         `json:"handle"`
	Metadata handleMetadata `json:"metadata"` // FeatureID/TaskID are feature/task slugs
	Result   executorResult `json:"result"`
}

// executorResult is the result.json payload written by the executor.
type executorResult struct {
	TerminalStatus string `json:"terminal_status"`
	PrURL          string `json:"pr_url"`
	BlockedReason  string `json:"blocked_reason"`
}

// Reaper drains go-owned completions from the broker and applies DB transitions.
// Construct via newReaper; call ReapCompleted for the default production instance.
type Reaper struct {
	brokerURL   string
	httpClient  *http.Client
	setInReview func(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID, prURL string) (bool, error)
	setBlocked  func(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID, reason string) (bool, error)
	slowLookup  func(ctx context.Context, pool *pgxpool.Pool, workspaceID uuid.UUID, featureName, taskName string) (*HandleEntry, error)
	appendLog   func(ctx context.Context, pool *pgxpool.Pool, workspaceID, featureUUID, taskUUID uuid.UUID, action, by, note string) error
}

func newReaper(brokerURL string, httpClient *http.Client) *Reaper {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &Reaper{
		brokerURL:   brokerURL,
		httpClient:  httpClient,
		setInReview: SetInReview,
		setBlocked:  SetBlocked,
		slowLookup:  dbLookupTaskBySlug,
		appendLog:   AppendLog,
	}
}

// ReapCompleted drains up to 50 go-owned completions from the broker and applies
// the appropriate DB status transition for each:
//   - terminal_status "in_review" → SetInReview (records pr_url)
//   - terminal_status "blocked"   → SetBlocked  (records blocked_reason)
//
// Unknown handles (not in HandleStore or DB) log a warning and are skipped.
// Each handle is deleted from the HandleStore after successful processing.
func ReapCompleted(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool, hs *HandleStore) error {
	workspaceUUID, err := uuid.Parse(cfg.WorkspaceID)
	if err != nil {
		return fmt.Errorf("reap: parse workspace id: %w", err)
	}
	return newReaper(cfg.BrokerURL, nil).reap(ctx, pool, hs, workspaceUUID)
}

func (r *Reaper) reap(ctx context.Context, pool *pgxpool.Pool, hs *HandleStore, workspaceUUID uuid.UUID) error {
	completions, err := r.listCompleted(ctx)
	if err != nil {
		return fmt.Errorf("reap: list-completed: %w", err)
	}

	for _, c := range completions {
		if err := r.processOne(ctx, pool, hs, workspaceUUID, c); err != nil {
			log.Warn().Err(err).Str("handle", c.Handle).Msg("reap: failed to process completion — skipping")
		}
	}
	return nil
}

func (r *Reaper) listCompleted(ctx context.Context) ([]completionRecord, error) {
	bodyJSON, err := json.Marshal(map[string]any{"owner": "go", "max": 50, "lock_ms": 30000})
	if err != nil {
		return nil, fmt.Errorf("marshal list-completed request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		r.brokerURL+"/list-completed", bytes.NewReader(bodyJSON))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("broker returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var records []completionRecord
	if err := json.Unmarshal(body, &records); err != nil {
		return nil, fmt.Errorf("unmarshal completions: %w", err)
	}
	return records, nil
}

func (r *Reaper) processOne(
	ctx context.Context,
	pool *pgxpool.Pool,
	hs *HandleStore,
	workspaceUUID uuid.UUID,
	c completionRecord,
) error {
	// Fast path: handle is tracked in the HandleStore.
	entry, ok := hs.Lookup(c.Handle)
	if !ok {
		// Slow path: resolve by feature_name + task_name from the DB.
		resolved, err := r.slowLookup(ctx, pool, workspaceUUID, c.Metadata.FeatureID, c.Metadata.TaskID)
		if err != nil {
			return fmt.Errorf("slow-path lookup handle=%q: %w", c.Handle, err)
		}
		if resolved == nil {
			log.Warn().
				Str("handle", c.Handle).
				Str("feature", c.Metadata.FeatureID).
				Str("task", c.Metadata.TaskID).
				Msg("reap: unknown handle — skipping")
			return nil
		}
		entry = *resolved
	}

	switch c.Result.TerminalStatus {
	case "in_review":
		if _, err := r.setInReview(ctx, pool, workspaceUUID, entry.TaskUUID, c.Result.PrURL); err != nil {
			return fmt.Errorf("SetInReview handle=%q: %w", c.Handle, err)
		}
	case "blocked":
		if _, err := r.setBlocked(ctx, pool, workspaceUUID, entry.TaskUUID, c.Result.BlockedReason); err != nil {
			return fmt.Errorf("SetBlocked handle=%q: %w", c.Handle, err)
		}
	default:
		log.Warn().
			Str("handle", c.Handle).
			Str("terminal_status", c.Result.TerminalStatus).
			Msg("reap: unrecognised terminal_status — skipping transition")
	}

	hs.Delete(c.Handle)

	if err := r.appendLog(ctx, pool, workspaceUUID, entry.FeatureUUID, entry.TaskUUID,
		"reap", "go-orchestrator", "completion reaped from broker"); err != nil {
		log.Warn().Err(err).Str("handle", c.Handle).Msg("reap: failed to append log entry")
	}

	return nil
}

// dbLookupTaskBySlug queries the DB for a task by its feature_name + task_name slugs.
// Returns nil (no error) when no matching row is found.
func dbLookupTaskBySlug(
	ctx context.Context,
	pool *pgxpool.Pool,
	workspaceID uuid.UUID,
	featureName, taskName string,
) (*HandleEntry, error) {
	var featureUUID, taskUUID uuid.UUID
	err := pool.QueryRow(ctx,
		`SELECT feature_id, task_id FROM workspace_tasks
		 WHERE workspace_id = $1 AND feature_name = $2 AND task_name = $3
		   AND owner = 'go'
		 LIMIT 1`,
		workspaceID, featureName, taskName,
	).Scan(&featureUUID, &taskUUID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("dbLookupTaskBySlug: %w", err)
	}
	return &HandleEntry{
		FeatureUUID: featureUUID,
		TaskUUID:    taskUUID,
		FeatureName: featureName,
		TaskName:    taskName,
	}, nil
}
