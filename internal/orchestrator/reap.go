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
	brokerURL          string
	httpClient         *http.Client
	setInReview        func(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID, prURL string) (bool, error)
	setBlocked         func(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID, reason, fromStatus string) (bool, error)
	setReadyMaxTurns   func(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID) (bool, error)
	getMaxTurnsCount   func(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID) (int32, error)
	slowLookup         func(ctx context.Context, pool *pgxpool.Pool, workspaceID uuid.UUID, featureName, taskName string) (*HandleEntry, error)
	appendLog          func(ctx context.Context, pool *pgxpool.Pool, workspaceID, featureUUID, taskUUID uuid.UUID, action, by, note string) error
	executorMaxRetries int
}

func newReaper(brokerURL string, httpClient *http.Client, executorMaxRetries int) *Reaper {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &Reaper{
		brokerURL:   brokerURL,
		httpClient:  httpClient,
		setInReview: SetInReview,
		setBlocked: func(ctx context.Context, pool *pgxpool.Pool, workspaceID, taskUUID uuid.UUID, reason, fromStatus string) (bool, error) {
			return SetBlockedWithDetails(ctx, pool, workspaceID, taskUUID, reason, "", fromStatus)
		},
		setReadyMaxTurns:   SetReadyFromMaxTurns,
		getMaxTurnsCount:   GetMaxTurnsRetryCount,
		slowLookup:         dbLookupTaskBySlug,
		appendLog:          AppendLog,
		executorMaxRetries: executorMaxRetries,
	}
}

// blockedFromStatusForKind returns the pre-block task status given a dispatch kind.
// impl/fix/rebase dispatch: task was in "in_progress" before dispatch.
// review dispatch: task was in "reviewing" before dispatch.
func blockedFromStatusForKind(kind string) string {
	if kind == "review" {
		return "reviewing"
	}
	return "in_progress"
}

// ReapCompleted drains up to 50 go-owned completions from the broker and applies
// the appropriate DB status transition for each:
//   - terminal_status "in_review"  → SetInReview (records pr_url; resets max_turns_retry_count)
//   - terminal_status "blocked"    → SetBlocked  (records blocked_reason)
//   - terminal_status "failed"     → SetBlocked  (DLQ spawn failure; records blocked_reason)
//   - terminal_status "max_turns"  → SetReadyFromMaxTurns if retries remain; else SetBlocked
//
// Unknown handles (not in HandleStore or DB) log a warning and are skipped.
// Each handle is deleted from the HandleStore after successful processing.
func ReapCompleted(ctx context.Context, cfg *config.Config, pool *pgxpool.Pool, hs *HandleStore) error {
	workspaceUUID, err := uuid.Parse(cfg.WorkspaceID)
	if err != nil {
		return fmt.Errorf("reap: parse workspace id: %w", err)
	}
	return newReaper(cfg.BrokerURL, nil, cfg.ExecutorMaxRetries).reap(ctx, pool, hs, workspaceUUID)
}

func (r *Reaper) reap(ctx context.Context, pool *pgxpool.Pool, hs *HandleStore, workspaceUUID uuid.UUID) error {
	completions, err := r.listCompleted(ctx)
	if err != nil {
		return fmt.Errorf("reap: list-completed: %w", err)
	}

	for _, c := range completions {
		if err := r.processOne(ctx, pool, hs, workspaceUUID, c); err != nil {
			log.Warn().Err(err).Str("handle", c.Handle).Msg("reap: failed to process completion — skipping")
			continue
		}
		// Commit consumption: removes the handle from the broker queue and registry.
		if err := r.ackCompletion(ctx, c.Handle); err != nil {
			log.Warn().Err(err).Str("handle", c.Handle).Msg("reap: ack failed — item will be redelivered after lock expires")
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
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Warn().Err(err).Msg("reap: close response body")
		}
	}()

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

// ackCompletion commits consumption of a handle by POST /ack.
// A non-2xx response is returned as an error but does not stop processing;
// the item will be redelivered by the broker after the visibility-timeout expires.
func (r *Reaper) ackCompletion(ctx context.Context, handle string) error {
	bodyJSON, err := json.Marshal(map[string]string{"handle": handle})
	if err != nil {
		return fmt.Errorf("marshal ack: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		r.brokerURL+"/ack", bytes.NewReader(bodyJSON))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Warn().Err(err).Msg("reap: close response body")
		}
	}()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("broker ack returned status %d", resp.StatusCode)
	}
	return nil
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
		fromStatus := blockedFromStatusForKind(c.Metadata.Kind)
		if _, err := r.setBlocked(ctx, pool, workspaceUUID, entry.TaskUUID, c.Result.BlockedReason, fromStatus); err != nil {
			return fmt.Errorf("SetBlocked handle=%q: %w", c.Handle, err)
		}
	case "failed":
		// DLQ spawn failure: dispatcher posted a synthetic "failed" after exhausting
		// delivery attempts. Treat as a block with the provided reason.
		reason := c.Result.BlockedReason
		if reason == "" {
			reason = "spawn_dlq_failed"
		}
		fromStatus := blockedFromStatusForKind(c.Metadata.Kind)
		if _, err := r.setBlocked(ctx, pool, workspaceUUID, entry.TaskUUID, reason, fromStatus); err != nil {
			return fmt.Errorf("SetBlocked(failed) handle=%q: %w", c.Handle, err)
		}
	case "max_turns":
		// Executor hit the model's max-turns limit. Retry up to executorMaxRetries;
		// after that, block the task so a human can intervene.
		if err := r.handleMaxTurns(ctx, pool, workspaceUUID, entry.TaskUUID, c.Handle); err != nil {
			return err
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

// handleMaxTurns applies the max-turns FSM edge:
//   - if max_turns_retry_count < executorMaxRetries → in_progress→ready + increment counter
//   - else → blocked (max_turns_exceeded)
//
// The getMaxTurnsCount read and the subsequent SetReadyFromMaxTurns / SetBlocked
// are not atomic: a concurrent transition could race, but SetReadyFromMaxTurns
// is guarded on status=in_progress and SetBlocked is guarded on non-terminal,
// so the worst case is a lost transition (the next cycle handles it) — no
// double-spawn.
func (r *Reaper) handleMaxTurns(
	ctx context.Context,
	pool *pgxpool.Pool,
	workspaceUUID, taskUUID uuid.UUID,
	handle string,
) error {
	count, err := r.getMaxTurnsCount(ctx, pool, workspaceUUID, taskUUID)
	if err != nil {
		return fmt.Errorf("handleMaxTurns: get count handle=%q: %w", handle, err)
	}

	if int(count) < r.executorMaxRetries {
		ok, err := r.setReadyMaxTurns(ctx, pool, workspaceUUID, taskUUID)
		if err != nil {
			return fmt.Errorf("SetReadyFromMaxTurns handle=%q: %w", handle, err)
		}
		if ok {
			log.Info().
				Str("handle", handle).
				Int32("retry", count+1).
				Int("max", r.executorMaxRetries).
				Msg("reap: max-turns reset — task returned to ready")
		}
		return nil
	}

	if _, err := r.setBlocked(ctx, pool, workspaceUUID, taskUUID, "max_turns_exceeded", "in_progress"); err != nil {
		return fmt.Errorf("SetBlocked(max_turns) handle=%q: %w", handle, err)
	}
	log.Info().
		Str("handle", handle).
		Int32("count", count).
		Int("max", r.executorMaxRetries).
		Msg("reap: max-turns cap reached — task blocked")
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
