package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/tiendv89/workflow-orchestrator/internal/config"
	db "github.com/tiendv89/workflow-orchestrator/internal/database/queries"
	gh "github.com/tiendv89/workflow-orchestrator/internal/github"
)

// goFeatureRow holds the minimal feature fields needed by the lifecycle step.
type goFeatureRow struct {
	ID          uuid.UUID // workspace_features.id (PK)
	FeatureID   uuid.UUID // workspace_features.feature_id (business-key UUID)
	FeatureName string
	Status      *string
}

// guardedUpdateFeatureStatus atomically transitions a workspace_feature's
// feature_status from fromStatus to toStatus.
// Returns (true, nil) on success, (false, nil) when the guard fires (status
// already changed), and (false, err) on DB error.
func guardedUpdateFeatureStatus(
	ctx context.Context,
	pool *pgxpool.Pool,
	featureID uuid.UUID,
	fromStatus, toStatus string,
) (bool, error) {
	const sql = `
UPDATE workspace_features
SET feature_status = $1, updated_at = now()
WHERE feature_id   = $2
  AND feature_status = $3
RETURNING id`

	var id uuid.UUID
	err := pool.QueryRow(ctx, sql, toStatus, featureID, fromStatus).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("guardedUpdateFeatureStatus: %w", err)
	}
	return true, nil
}

// listGoFeaturesInLifecycle returns go-owned features whose feature_status is
// ready_for_implementation or in_implementation — the two states managed here.
func listGoFeaturesInLifecycle(
	ctx context.Context,
	pool *pgxpool.Pool,
	workspaceID uuid.UUID,
) ([]goFeatureRow, error) {
	const sql = `
SELECT id, feature_id, feature_name, feature_status
FROM workspace_features
WHERE workspace_id  = $1
  AND owner         = 'go'
  AND feature_status IN ('ready_for_implementation', 'in_implementation')
ORDER BY created_at ASC`

	rows, err := pool.Query(ctx, sql, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("listGoFeaturesInLifecycle: %w", err)
	}
	defer rows.Close()

	var features []goFeatureRow
	for rows.Next() {
		var f goFeatureRow
		if err := rows.Scan(&f.ID, &f.FeatureID, &f.FeatureName, &f.Status); err != nil {
			return nil, fmt.Errorf("listGoFeaturesInLifecycle: scan: %w", err)
		}
		features = append(features, f)
	}
	return features, rows.Err()
}

// isAnyTaskDispatched returns true if at least one task for the feature has
// moved past "ready" (i.e., is currently being processed or was processed).
// This is the trigger for ready_for_implementation → in_implementation.
func isAnyTaskDispatched(ctx context.Context, pool *pgxpool.Pool, featureID uuid.UUID) (bool, error) {
	const sql = `
SELECT EXISTS (
    SELECT 1 FROM workspace_tasks
    WHERE feature_id = $1
      AND owner      = 'go'
      AND status NOT IN ('todo', 'ready', 'done', 'cancelled')
)`

	var exists bool
	if err := pool.QueryRow(ctx, sql, featureID).Scan(&exists); err != nil {
		return false, fmt.Errorf("isAnyTaskDispatched: %w", err)
	}
	return exists, nil
}

// areAllTasksTerminal returns true when every task for the feature is either
// done or cancelled — the predicate for firing the handoff trigger.
func areAllTasksTerminal(ctx context.Context, pool *pgxpool.Pool, featureID uuid.UUID) (bool, error) {
	const sql = `
SELECT NOT EXISTS (
    SELECT 1 FROM workspace_tasks
    WHERE feature_id = $1
      AND owner      = 'go'
      AND status NOT IN ('done', 'cancelled')
)`

	var allTerminal bool
	if err := pool.QueryRow(ctx, sql, featureID).Scan(&allTerminal); err != nil {
		return false, fmt.Errorf("areAllTasksTerminal: %w", err)
	}
	return allTerminal, nil
}

// getDistinctTaskRepos returns distinct repo IDs referenced by the feature's tasks.
func getDistinctTaskRepos(ctx context.Context, pool *pgxpool.Pool, featureID uuid.UUID) ([]string, error) {
	const sql = `
SELECT DISTINCT repo
FROM workspace_tasks
WHERE feature_id = $1
  AND owner      = 'go'
  AND repo IS NOT NULL
ORDER BY repo`

	rows, err := pool.Query(ctx, sql, featureID)
	if err != nil {
		return nil, fmt.Errorf("getDistinctTaskRepos: %w", err)
	}
	defer rows.Close()

	var repos []string
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			return nil, fmt.Errorf("getDistinctTaskRepos: scan: %w", err)
		}
		repos = append(repos, r)
	}
	return repos, rows.Err()
}

// TriggerHandoff creates the handoff row, one handoff_prs row per distinct repo,
// opens draft feature→main PRs, sets the mgmt-repo status PR, and transitions
// the feature to in_handoff.
//
// The UNIQUE(feature_id) constraint on handoffs is the multi-instance guard —
// if another orchestrator instance already inserted the row, InsertHandoff
// returns nil (ON CONFLICT DO NOTHING) and we skip the trigger.
func TriggerHandoff(
	ctx context.Context,
	pool *pgxpool.Pool,
	cfg *config.Config,
	ghCreator gh.PRCreator,
	workspaceID, featureID uuid.UUID,
	featureName string,
) error {
	q := db.New(pool)

	// Insert the handoffs row (idempotent — ON CONFLICT DO NOTHING).
	handoff, err := q.InsertHandoff(ctx, db.InsertHandoffParams{
		WorkspaceID: workspaceID,
		FeatureID:   featureID,
	})
	if err != nil {
		return fmt.Errorf("TriggerHandoff: InsertHandoff: %w", err)
	}
	if handoff == nil {
		// Another instance already created the handoff — skip.
		log.Debug().
			Str("feature", featureName).
			Msg("TriggerHandoff: handoff row already exists — skipping (another instance won)")
		return nil
	}

	// Transition feature_status → in_handoff.
	ok, err := guardedUpdateFeatureStatus(ctx, pool, featureID, "in_implementation", "in_handoff")
	if err != nil {
		return fmt.Errorf("TriggerHandoff: set in_handoff: %w", err)
	}
	if !ok {
		log.Warn().
			Str("feature", featureName).
			Msg("TriggerHandoff: feature not in in_implementation — status update skipped")
	}

	// Get distinct repos for this feature.
	repos, err := getDistinctTaskRepos(ctx, pool, featureID)
	if err != nil {
		return fmt.Errorf("TriggerHandoff: get repos: %w", err)
	}

	featureBranch := FeatureBranchName(featureName)

	// Open a draft feature→main PR for each repo.
	for _, repoID := range repos {
		if err := createHandoffPR(ctx, q, cfg, ghCreator, pool, handoff.ID, workspaceID, repoID, featureName, featureBranch); err != nil {
			log.Error().Err(err).
				Str("feature", featureName).
				Str("repo", repoID).
				Msg("TriggerHandoff: failed to create handoff PR — continuing with other repos")
		}
	}

	// Open the mgmt-repo status PR.
	mgmtPRURL, err := createMgmtHandoffPR(ctx, cfg, ghCreator, featureName, featureBranch)
	if err != nil {
		log.Warn().Err(err).
			Str("feature", featureName).
			Msg("TriggerHandoff: failed to create mgmt PR") // TODO(slack)
	} else if mgmtPRURL != "" {
		if err := q.UpdateHandoffMgmtPRURL(ctx, db.UpdateHandoffMgmtPRURLParams{
			ID:        handoff.ID,
			MgmtPrURL: &mgmtPRURL,
		}); err != nil {
			log.Warn().Err(err).Msg("TriggerHandoff: update mgmt_pr_url failed")
		}
	}

	log.Info().
		Str("feature", featureName).
		Strs("repos", repos).
		Msg("TriggerHandoff: handoff triggered") // TODO(slack)

	return nil
}

// createHandoffPR opens a draft PR for one repo and inserts a handoff_prs row.
func createHandoffPR(
	ctx context.Context,
	q *db.Queries,
	cfg *config.Config,
	ghCreator gh.PRCreator,
	pool *pgxpool.Pool,
	handoffID, workspaceID uuid.UUID,
	repoID, featureName, featureBranch string,
) error {
	// Look up the repo URL and base branch.
	wsUUID := workspaceID
	repoRow, err := q.GetWorkspaceRepo(ctx, db.GetWorkspaceRepoParams{
		WorkspaceID: wsUUID,
		RepoID:      repoID,
	})
	if err != nil {
		// Record as skipped_no_branch if repo not found.
		if errors.Is(err, pgx.ErrNoRows) {
			log.Warn().
				Str("repo", repoID).
				Str("feature", featureName).
				Msg("TriggerHandoff: repo not in workspace_repos — skipping") // TODO(slack)
			status := "skipped_no_branch"
			_, _ = q.InsertHandoffPR(ctx, db.InsertHandoffPRParams{
				HandoffID: handoffID,
				Repo:      repoID,
				PrURL:     nil,
				Status:    status,
			})
			return nil
		}
		return fmt.Errorf("GetWorkspaceRepo(%s): %w", repoID, err)
	}

	if repoRow.RepoURL == nil || *repoRow.RepoURL == "" {
		log.Warn().
			Str("repo", repoID).
			Str("feature", featureName).
			Msg("TriggerHandoff: repo has no URL — skipping") // TODO(slack)
		status := "skipped_no_branch"
		_, _ = q.InsertHandoffPR(ctx, db.InsertHandoffPRParams{
			HandoffID: handoffID,
			Repo:      repoID,
			PrURL:     nil,
			Status:    status,
		})
		return nil
	}

	repoURL := *repoRow.RepoURL
	baseBranch := cfg.BaseBranch
	if repoRow.BaseBranch != nil && *repoRow.BaseBranch != "" {
		baseBranch = *repoRow.BaseBranch
	}

	// Check if the feature branch exists in this repo.
	if ghCreator != nil {
		exists, err := ghCreator.BranchExists(ctx, repoURL, featureBranch)
		if err != nil {
			log.Warn().Err(err).
				Str("repo", repoID).
				Str("branch", featureBranch).
				Msg("TriggerHandoff: branch existence check failed — skipping") // TODO(slack)
			status := "skipped_no_branch"
			_, _ = q.InsertHandoffPR(ctx, db.InsertHandoffPRParams{
				HandoffID: handoffID,
				Repo:      repoID,
				PrURL:     nil,
				Status:    status,
			})
			return nil
		}
		if !exists {
			log.Warn().
				Str("repo", repoID).
				Str("feature", featureName).
				Str("branch", featureBranch).
				Msg("TriggerHandoff: feature branch missing — skipping") // TODO(slack)
			status := "skipped_no_branch"
			_, _ = q.InsertHandoffPR(ctx, db.InsertHandoffPRParams{
				HandoffID: handoffID,
				Repo:      repoID,
				PrURL:     nil,
				Status:    status,
			})
			return nil
		}
	}

	// Open the draft PR.
	title := fmt.Sprintf("handoff(%s): merge feature branch into %s", featureName, baseBranch)
	body := fmt.Sprintf("Automated handoff PR for feature `%s`.\n\nMerges `%s` → `%s`.",
		featureName, featureBranch, baseBranch)

	var prURL string
	if ghCreator != nil {
		url, err := ghCreator.CreatePR(ctx, repoURL, featureBranch, baseBranch, title, body, true)
		if err != nil {
			log.Error().Err(err).
				Str("repo", repoID).
				Str("feature", featureName).
				Msg("TriggerHandoff: CreatePR failed — recording skipped_no_branch")
			status := "skipped_no_branch"
			_, _ = q.InsertHandoffPR(ctx, db.InsertHandoffPRParams{
				HandoffID: handoffID,
				Repo:      repoID,
				PrURL:     nil,
				Status:    status,
			})
			return nil
		}
		prURL = url
	}

	status := "open"
	_, err = q.InsertHandoffPR(ctx, db.InsertHandoffPRParams{
		HandoffID: handoffID,
		Repo:      repoID,
		PrURL:     &prURL,
		Status:    status,
	})
	if err != nil {
		return fmt.Errorf("InsertHandoffPR(%s): %w", repoID, err)
	}

	log.Info().
		Str("repo", repoID).
		Str("pr_url", prURL).
		Str("feature", featureName).
		Msg("TriggerHandoff: draft handoff PR opened")
	return nil
}

// createMgmtHandoffPR opens a draft PR on the management repo for this feature.
// Returns the PR URL, or "" if cfg.ManagementRepo is unset.
func createMgmtHandoffPR(
	ctx context.Context,
	cfg *config.Config,
	ghCreator gh.PRCreator,
	featureName, featureBranch string,
) (string, error) {
	if cfg.ManagementRepo == "" || ghCreator == nil {
		return "", nil
	}

	title := fmt.Sprintf("handoff(%s): feature branch → main", featureName)
	body := fmt.Sprintf("Management-repo status PR for handoff of feature `%s`.\n\nMerges `%s` → `%s`.",
		featureName, featureBranch, cfg.BaseBranch)

	url, err := ghCreator.CreatePR(ctx, cfg.ManagementRepo, featureBranch, cfg.BaseBranch, title, body, true)
	if err != nil {
		return "", fmt.Errorf("createMgmtHandoffPR: %w", err)
	}
	return url, nil
}

// RunFeatureLifecycle runs the feature lifecycle step for one poll cycle:
//  1. For ready_for_implementation go features where any task is dispatched →
//     transition to in_implementation (first-dispatch rule).
//  2. For in_implementation go features where all tasks are terminal →
//     trigger handoff (if not already triggered).
func RunFeatureLifecycle(
	ctx context.Context,
	pool *pgxpool.Pool,
	cfg *config.Config,
	ghCreator gh.PRCreator,
	workspaceID uuid.UUID,
) error {
	features, err := listGoFeaturesInLifecycle(ctx, pool, workspaceID)
	if err != nil {
		return fmt.Errorf("RunFeatureLifecycle: list features: %w", err)
	}

	for _, f := range features {
		status := ""
		if f.Status != nil {
			status = *f.Status
		}

		switch status {
		case "ready_for_implementation":
			// Check if any task is dispatched → in_implementation.
			dispatched, err := isAnyTaskDispatched(ctx, pool, f.FeatureID)
			if err != nil {
				log.Error().Err(err).Str("feature", f.FeatureName).
					Msg("RunFeatureLifecycle: isAnyTaskDispatched failed")
				continue
			}
			if !dispatched {
				continue
			}
			ok, err := guardedUpdateFeatureStatus(ctx, pool, f.FeatureID,
				"ready_for_implementation", "in_implementation")
			if err != nil {
				log.Error().Err(err).Str("feature", f.FeatureName).
					Msg("RunFeatureLifecycle: set in_implementation failed")
				continue
			}
			if ok {
				log.Info().Str("feature", f.FeatureName).
					Msg("RunFeatureLifecycle: feature → in_implementation")
			}

		case "in_implementation":
			// Check if all tasks are terminal → handoff trigger.
			terminal, err := areAllTasksTerminal(ctx, pool, f.FeatureID)
			if err != nil {
				log.Error().Err(err).Str("feature", f.FeatureName).
					Msg("RunFeatureLifecycle: areAllTasksTerminal failed")
				continue
			}
			if !terminal {
				continue
			}
			if err := TriggerHandoff(ctx, pool, cfg, ghCreator, workspaceID, f.FeatureID, f.FeatureName); err != nil {
				log.Error().Err(err).Str("feature", f.FeatureName).
					Msg("RunFeatureLifecycle: TriggerHandoff failed")
			}
		}
	}
	return nil
}

// CheckAndFinalizeHandoffs scans open handoffs and finalizes any where all
// handoff_prs rows are merged. Finalization: merges the mgmt PR → sets
// handoff.status='finalized' → sets feature_status='done'.
func CheckAndFinalizeHandoffs(
	ctx context.Context,
	pool *pgxpool.Pool,
	ghMerger gh.PRMerger,
	workspaceID uuid.UUID,
) error {
	// List open handoffs for this workspace.
	const listSQL = `
SELECT h.id, h.feature_id, h.mgmt_pr_url, wf.feature_name
FROM handoffs h
JOIN workspace_features wf ON wf.feature_id = h.feature_id
WHERE h.status = 'open'
  AND wf.workspace_id = $1
  AND wf.owner = 'go'
ORDER BY h.created_at ASC`

	rows, err := pool.Query(ctx, listSQL, workspaceID)
	if err != nil {
		return fmt.Errorf("CheckAndFinalizeHandoffs: list handoffs: %w", err)
	}
	defer rows.Close()

	type handoffEntry struct {
		ID          uuid.UUID
		FeatureID   uuid.UUID
		MgmtPRURL   *string
		FeatureName string
	}
	var handoffs []handoffEntry
	for rows.Next() {
		var h handoffEntry
		if err := rows.Scan(&h.ID, &h.FeatureID, &h.MgmtPRURL, &h.FeatureName); err != nil {
			return fmt.Errorf("CheckAndFinalizeHandoffs: scan: %w", err)
		}
		handoffs = append(handoffs, h)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("CheckAndFinalizeHandoffs: rows err: %w", err)
	}

	q := db.New(pool)
	for _, h := range handoffs {
		if err := maybeFinalize(ctx, pool, q, ghMerger, h.ID, h.FeatureID, h.FeatureName, h.MgmtPRURL); err != nil {
			log.Error().Err(err).
				Str("feature", h.FeatureName).
				Msg("CheckAndFinalizeHandoffs: maybeFinalize failed")
		}
	}
	return nil
}

// maybeFinalize checks if all non-skipped handoff_prs are merged and, if so,
// finalizes the handoff.
func maybeFinalize(
	ctx context.Context,
	pool *pgxpool.Pool,
	q *db.Queries,
	ghMerger gh.PRMerger,
	handoffID, featureID uuid.UUID,
	featureName string,
	mgmtPRURL *string,
) error {
	prs, err := q.ListHandoffPRsByHandoff(ctx, handoffID)
	if err != nil {
		return fmt.Errorf("maybeFinalize: list PRs: %w", err)
	}

	for _, pr := range prs {
		if pr.Status == "skipped_no_branch" {
			continue // skipped repos don't block finalization
		}
		if pr.Status != "merged" {
			return nil // not all merged yet
		}
	}

	// All non-skipped PRs are merged. Proceed with finalization.
	log.Info().
		Str("feature", featureName).
		Msg("CheckAndFinalizeHandoffs: all handoff PRs merged — finalizing")

	// Merge the mgmt PR if set.
	if mgmtPRURL != nil && *mgmtPRURL != "" && ghMerger != nil {
		if err := ghMerger.MergePR(ctx, *mgmtPRURL); err != nil {
			log.Warn().Err(err).
				Str("feature", featureName).
				Str("mgmt_pr_url", *mgmtPRURL).
				Msg("CheckAndFinalizeHandoffs: MergePR failed — will retry next cycle")
			return nil // don't finalize if mgmt PR merge failed
		}
	}

	// Finalize the handoff row.
	if err := q.FinalizeHandoff(ctx, handoffID); err != nil {
		return fmt.Errorf("maybeFinalize: FinalizeHandoff: %w", err)
	}

	// Set feature_status → done.
	ok, err := guardedUpdateFeatureStatus(ctx, pool, featureID, "in_handoff", "done")
	if err != nil {
		return fmt.Errorf("maybeFinalize: set done: %w", err)
	}
	if ok {
		log.Info().
			Str("feature", featureName).
			Msg("CheckAndFinalizeHandoffs: feature → done") // TODO(slack)
	}

	return nil
}
