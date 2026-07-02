package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/tiendv89/workflow-orchestrator/internal/config"
	db "github.com/tiendv89/workflow-orchestrator/internal/database/queries"
	gh "github.com/tiendv89/workflow-orchestrator/internal/github"
)

// SetHandoffPRConflicted marks a handoff_pr as 'conflicted'.
// Guard: only updates when conflict_state is NOT 'resolving' and status is 'open'.
// Returns (true, nil) on update, (false, nil) when guard fires, (false, err) on error.
func SetHandoffPRConflicted(ctx context.Context, pool *pgxpool.Pool, handoffPRID uuid.UUID) (bool, error) {
	const sql = `
UPDATE handoff_prs SET
    conflict_state = 'conflicted'
WHERE id             = $1
  AND conflict_state != 'resolving'
  AND status         = 'open'
RETURNING id`

	var id uuid.UUID
	err := pool.QueryRow(ctx, sql, handoffPRID).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("SetHandoffPRConflicted: %w", err)
	}
	return true, nil
}

// SetHandoffPRResolving claims the rebase slot for a conflicted handoff PR.
// Transitions conflict_state from 'conflicted' to 'resolving' with dispatch-in columns.
// First-write-wins.
func SetHandoffPRResolving(
	ctx context.Context,
	pool *pgxpool.Pool,
	handoffPRID uuid.UUID,
	handle, nonce string,
) (bool, error) {
	const sql = `
UPDATE handoff_prs SET
    conflict_state     = 'resolving',
    dispatch_handle    = $2,
    dispatch_nonce     = $3,
    dispatched_at      = now(),
    reenqueue_attempts = 0
WHERE id             = $1
  AND conflict_state = 'conflicted'
  AND status         = 'open'
RETURNING id`

	var id uuid.UUID
	err := pool.QueryRow(ctx, sql, handoffPRID, handle, nonce).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("SetHandoffPRResolving: %w", err)
	}
	return true, nil
}

// SetHandoffPRResolved marks a successful rebase: conflict_state 'resolving' → 'resolved'.
// Clears dispatch columns and resets rebase_attempts.
func SetHandoffPRResolved(ctx context.Context, pool *pgxpool.Pool, handoffPRID uuid.UUID) (bool, error) {
	const sql = `
UPDATE handoff_prs SET
    conflict_state     = 'resolved',
    rebase_attempts    = 0,
    dispatch_handle    = NULL,
    dispatch_nonce     = NULL,
    dispatched_at      = NULL
WHERE id             = $1
  AND conflict_state = 'resolving'
RETURNING id`

	var id uuid.UUID
	err := pool.QueryRow(ctx, sql, handoffPRID).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("SetHandoffPRResolved: %w", err)
	}
	return true, nil
}

// MarkHandoffPRRebaseRetry transitions conflict_state from 'resolving' back to
// 'conflicted' and increments rebase_attempts. Used on a retriable failure.
func MarkHandoffPRRebaseRetry(ctx context.Context, pool *pgxpool.Pool, handoffPRID uuid.UUID) (bool, error) {
	const sql = `
UPDATE handoff_prs SET
    conflict_state     = 'conflicted',
    rebase_attempts    = rebase_attempts + 1,
    dispatch_handle    = NULL,
    dispatch_nonce     = NULL,
    dispatched_at      = NULL
WHERE id             = $1
  AND conflict_state = 'resolving'
RETURNING id`

	var id uuid.UUID
	err := pool.QueryRow(ctx, sql, handoffPRID).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("MarkHandoffPRRebaseRetry: %w", err)
	}
	return true, nil
}

// rollbackHandoffPRResolving clears 'resolving' state without incrementing
// rebase_attempts. Used when broker dispatch fails before the rebase ran.
func rollbackHandoffPRResolving(ctx context.Context, pool *pgxpool.Pool, handoffPRID uuid.UUID) (bool, error) {
	const sql = `
UPDATE handoff_prs SET
    conflict_state  = 'conflicted',
    dispatch_handle = NULL,
    dispatch_nonce  = NULL,
    dispatched_at   = NULL
WHERE id             = $1
  AND conflict_state = 'resolving'
RETURNING id`

	var id uuid.UUID
	err := pool.QueryRow(ctx, sql, handoffPRID).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("rollbackHandoffPRResolving: %w", err)
	}
	return true, nil
}

// FindConflictedHandoffPRs returns open handoff_prs with conflict_state='conflicted'
// whose rebase_attempts is below the cap. PRs at or above the cap are excluded so
// DispatchHandoffPRRebase stops re-dispatching them after the cap is reached.
func FindConflictedHandoffPRs(ctx context.Context, pool *pgxpool.Pool, maxRebaseAttempts int) ([]db.HandoffPr, error) {
	const sql = `
SELECT id, handoff_id, repo, pr_url, status, conflict_state, rebase_attempts,
       dispatch_handle, dispatch_nonce, dispatched_at, reenqueue_attempts, created_at
FROM handoff_prs
WHERE conflict_state   = 'conflicted'
  AND status           = 'open'
  AND rebase_attempts  < $1
ORDER BY created_at ASC`

	rows, err := pool.Query(ctx, sql, maxRebaseAttempts)
	if err != nil {
		return nil, fmt.Errorf("FindConflictedHandoffPRs: %w", err)
	}
	defer rows.Close()

	var prs []db.HandoffPr
	for rows.Next() {
		var p db.HandoffPr
		if err := rows.Scan(
			&p.ID, &p.HandoffID, &p.Repo, &p.PrURL, &p.Status,
			&p.ConflictState, &p.RebaseAttempts,
			&p.DispatchHandle, &p.DispatchNonce, &p.DispatchedAt, &p.ReenqueueAttempts,
			&p.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("FindConflictedHandoffPRs: scan: %w", err)
		}
		prs = append(prs, p)
	}
	return prs, rows.Err()
}

// HandleHandoffPRRebaseCompletion processes a completed rebase for a handoff PR.
// success=true → SetHandoffPRResolved.
// success=false → retry (below cap) or stay conflicted + Slack TODO (at cap).
// Handoff PR rebases don't block (Path B semantics only — human merges).
func HandleHandoffPRRebaseCompletion(
	ctx context.Context,
	pool *pgxpool.Pool,
	handoffPRID uuid.UUID,
	success bool,
	maxRebaseAttempts int,
) error {
	if success {
		ok, err := SetHandoffPRResolved(ctx, pool, handoffPRID)
		if err != nil {
			return fmt.Errorf("HandleHandoffPRRebaseCompletion: SetHandoffPRResolved: %w", err)
		}
		if !ok {
			log.Warn().
				Str("handoff_pr_id", handoffPRID.String()).
				Msg("HandleHandoffPRRebaseCompletion: SetHandoffPRResolved no-op")
		}
		return nil
	}

	// Failure path: read current rebase_attempts.
	var rebaseAttempts int32
	err := pool.QueryRow(ctx,
		`SELECT rebase_attempts FROM handoff_prs WHERE id = $1`,
		handoffPRID,
	).Scan(&rebaseAttempts)
	if err != nil {
		return fmt.Errorf("HandleHandoffPRRebaseCompletion: fetch attempts: %w", err)
	}

	nextAttempts := int(rebaseAttempts) + 1

	if nextAttempts >= maxRebaseAttempts {
		// Cap reached: stay conflicted + Slack TODO. No block for handoff PRs.
		if _, err := MarkHandoffPRRebaseRetry(ctx, pool, handoffPRID); err != nil {
			return fmt.Errorf("HandleHandoffPRRebaseCompletion: MarkRebaseRetry (cap): %w", err)
		}
		log.Warn().
			Str("handoff_pr_id", handoffPRID.String()).
			Int("rebase_attempts", nextAttempts).
			Msg("HandleHandoffPRRebaseCompletion: rebase cap reached — staying conflicted; human must resolve") // TODO(slack)
		return nil
	}

	// Below cap: retry.
	if _, err := MarkHandoffPRRebaseRetry(ctx, pool, handoffPRID); err != nil {
		return fmt.Errorf("HandleHandoffPRRebaseCompletion: MarkRebaseRetry: %w", err)
	}
	return nil
}

// handoffPRDispatchInfo holds the context needed to dispatch a handoff PR rebase.
type handoffPRDispatchInfo struct {
	pr          db.HandoffPr
	featureID   uuid.UUID
	featureName string
	repoURL     string
	baseBranch  string
}

// getHandoffPRDispatchInfo resolves repo URL, base branch, and feature name
// for a handoff PR from the DB.
func getHandoffPRDispatchInfo(
	ctx context.Context,
	pool *pgxpool.Pool,
	workspaceID uuid.UUID,
	pr db.HandoffPr,
) (*handoffPRDispatchInfo, error) {
	// Look up the handoff → feature_id + feature_name.
	var featureID uuid.UUID
	var featureName string
	err := pool.QueryRow(ctx, `
SELECT wf.feature_id, wf.feature_name
FROM handoffs h
JOIN workspace_features wf ON wf.feature_id = h.feature_id
WHERE h.id = $1`,
		pr.HandoffID,
	).Scan(&featureID, &featureName)
	if err != nil {
		return nil, fmt.Errorf("getHandoffPRDispatchInfo: feature lookup: %w", err)
	}

	// Look up the repo URL and base branch.
	var repoURL, baseBranch string
	err = pool.QueryRow(ctx, `
SELECT COALESCE(repo_url,''), COALESCE(base_branch,$2)
FROM workspace_repos
WHERE workspace_id = $1 AND repo_id = $3`,
		workspaceID, "main", pr.Repo,
	).Scan(&repoURL, &baseBranch)
	if err != nil {
		return nil, fmt.Errorf("getHandoffPRDispatchInfo: repo lookup: %w", err)
	}

	return &handoffPRDispatchInfo{
		pr:          pr,
		featureID:   featureID,
		featureName: featureName,
		repoURL:     repoURL,
		baseBranch:  baseBranch,
	}, nil
}

// DispatchHandoffPRRebase claims the rebase slot and enqueues a rebase job for
// a conflicted handoff PR. Returns (true, nil) on success, (false, nil) if
// another agent won the claim, and (false, err) on error.
func DispatchHandoffPRRebase(
	ctx context.Context,
	cfg *config.Config,
	pool *pgxpool.Pool,
	hs *HandleStore,
	dispatcher *Dispatcher,
	workspaceID uuid.UUID,
	pr db.HandoffPr,
) (bool, error) {
	info, err := getHandoffPRDispatchInfo(ctx, pool, workspaceID, pr)
	if err != nil {
		return false, fmt.Errorf("DispatchHandoffPRRebase: %w", err)
	}

	handle := uuid.New().String()
	nonce := uuid.New().String()

	// Guarded claim: conflict_state 'conflicted' → 'resolving'.
	ok, err := SetHandoffPRResolving(ctx, pool, pr.ID, handle, nonce)
	if err != nil {
		return false, fmt.Errorf("DispatchHandoffPRRebase: SetHandoffPRResolving: %w", err)
	}
	if !ok {
		return false, nil // another agent won
	}

	featureBranch := FeatureBranchName(info.featureName)
	now := time.Now().UTC().Format(time.RFC3339)

	// Build and enqueue the dispatch job directly (not via DispatchWithNonce
	// which is designed for tasks).
	job := dispatchJob{
		Handle:             handle,
		Nonce:              nonce,
		Kind:               "handoff_rebase",
		TaskID:             pr.Repo,          // repo name as task slug for broker metadata
		FeatureID:          info.featureName, // feature name as feature slug
		WorkspaceID:        cfg.WorkspaceID,
		TaskRepoURL:        info.repoURL,
		TaskRepoBranch:     featureBranch,   // the feature branch being rebased
		TaskBaseBranch:     info.baseBranch, // base (main) to rebase onto
		TaskRepoBaseBranch: info.baseBranch,
		MgmtRepoURL:        cfg.ManagementRepo,
		CallbackURL:        cfg.BrokerURL + "/callback",
		EnqueuedAt:         now,
	}

	// Register with the broker.
	fakeTask := db.WorkspaceTask{
		FeatureName: info.featureName,
		TaskName:    pr.Repo,
		FeatureID:   info.featureID,
		TaskID:      pr.ID, // use handoff PR ID as task UUID for broker metadata
	}
	if err := dispatcher.registerHandleWithKind(ctx, handle, fakeTask, cfg.OrganizationID, nonce, now, "handoff_rebase"); err != nil {
		if _, rbErr := rollbackHandoffPRResolving(ctx, pool, pr.ID); rbErr != nil {
			log.Warn().Err(rbErr).Str("handoff_pr_id", pr.ID.String()).
				Msg("DispatchHandoffPRRebase: rollback failed")
		}
		return false, fmt.Errorf("DispatchHandoffPRRebase: broker register: %w", err)
	}

	// Enqueue to Redis.
	if err := dispatcher.enqueueHandoffJob(ctx, job); err != nil {
		if _, rbErr := rollbackHandoffPRResolving(ctx, pool, pr.ID); rbErr != nil {
			log.Warn().Err(rbErr).Str("handoff_pr_id", pr.ID.String()).
				Msg("DispatchHandoffPRRebase: rollback failed")
		}
		return false, fmt.Errorf("DispatchHandoffPRRebase: enqueue: %w", err)
	}

	// Register in handle store for reap.
	prID := pr.ID
	hs.Register(handle, HandleEntry{
		FeatureUUID: info.featureID,
		FeatureName: info.featureName,
		TaskName:    pr.Repo,
		HandoffPRID: &prID,
	})

	log.Info().
		Str("feature", info.featureName).
		Str("repo", pr.Repo).
		Str("handle", handle).
		Msg("DispatchHandoffPRRebase: handoff PR rebase dispatched")
	return true, nil
}

// PollHandoffPRs fetches GitHub status for all open handoff PRs and updates the DB.
// Merged → status='merged'; CONFLICTING → conflict_state='conflicted'.
// Called after the handoff-PR rebase loop in the cycle.
func PollHandoffPRs(ctx context.Context, ghClient gh.PRGetter, pool *pgxpool.Pool) error {
	const sql = `
SELECT id, handoff_id, repo, pr_url, status, conflict_state, rebase_attempts,
       dispatch_handle, dispatch_nonce, dispatched_at, reenqueue_attempts, created_at
FROM handoff_prs
WHERE status = 'open'
  AND pr_url IS NOT NULL
ORDER BY created_at ASC`

	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return fmt.Errorf("PollHandoffPRs: query: %w", err)
	}
	defer rows.Close()

	var prs []db.HandoffPr
	for rows.Next() {
		var p db.HandoffPr
		if err := rows.Scan(
			&p.ID, &p.HandoffID, &p.Repo, &p.PrURL, &p.Status,
			&p.ConflictState, &p.RebaseAttempts,
			&p.DispatchHandle, &p.DispatchNonce, &p.DispatchedAt, &p.ReenqueueAttempts,
			&p.CreatedAt,
		); err != nil {
			return fmt.Errorf("PollHandoffPRs: scan: %w", err)
		}
		prs = append(prs, p)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("PollHandoffPRs: rows: %w", err)
	}

	for _, pr := range prs {
		if pr.PrURL == nil || *pr.PrURL == "" {
			continue
		}
		if err := processHandoffPRPoll(ctx, ghClient, pool, pr); err != nil {
			log.Error().Err(err).
				Str("pr_url", *pr.PrURL).
				Str("repo", pr.Repo).
				Msg("PollHandoffPRs: poll failed for PR — continuing")
		}
	}
	return nil
}

func processHandoffPRPoll(
	ctx context.Context,
	ghClient gh.PRGetter,
	pool *pgxpool.Pool,
	pr db.HandoffPr,
) error {
	status, err := ghClient.GetPR(ctx, *pr.PrURL)
	if err != nil {
		log.Warn().Err(err).Str("pr_url", *pr.PrURL).Msg("PollHandoffPRs: GetPR failed — skipping")
		return nil
	}

	if status.Merged {
		_, err := pool.Exec(ctx,
			`UPDATE handoff_prs SET status='merged' WHERE id=$1 AND status='open'`,
			pr.ID,
		)
		if err != nil {
			return fmt.Errorf("processHandoffPRPoll: mark merged: %w", err)
		}
		log.Info().
			Str("pr_url", *pr.PrURL).
			Str("repo", pr.Repo).
			Msg("PollHandoffPRs: handoff PR merged")
		return nil
	}

	switch status.Mergeable {
	case "CONFLICTING":
		if pr.ConflictState == ConflictStateResolving {
			return nil // rebase in-flight
		}
		ok, err := SetHandoffPRConflicted(ctx, pool, pr.ID)
		if err != nil {
			return fmt.Errorf("processHandoffPRPoll: SetHandoffPRConflicted: %w", err)
		}
		if ok {
			log.Info().
				Str("pr_url", *pr.PrURL).
				Str("repo", pr.Repo).
				Msg("PollHandoffPRs: conflict detected — conflict_state=conflicted")
		}
	case "UNKNOWN":
		// GitHub hasn't computed mergeability yet; recheck next cycle.
	}
	return nil
}
