package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/tiendv89/workflow-orchestrator/internal/database/queries"
	gh "github.com/tiendv89/workflow-orchestrator/internal/github"
)

// PollMergedPRs scans all go-owned tasks in "in_review", "reviewing", or
// "review_passed" status that have a PR URL stored in the pr JSONB field.
//
// For each task it fetches the PR from GitHub and applies:
//   - Merged (merged=true): SetDoneFromMergedPR — ground-truth: the merged PR
//     supersedes any in-flight reviewer result, regardless of task status.
//     Implements the merged-PR-is-ground-truth rule: a reviewer can auto-merge
//     the PR while the task is still "reviewing" (verdict not yet reaped).
//     Without checking "reviewing" and "review_passed" here, such tasks would
//     be stuck until the verdict is reaped or the reconciler fires.
//   - Conflicting (mergeable="CONFLICTING"): SetConflicted — marks the conflict
//     for the rebase dispatch loop.
//   - Unknown (mergeable="UNKNOWN"): skip — GitHub has not finished computing
//     mergeability; recheck on the next poll cycle.
//   - Open, no conflict: no action.
//
// GitHub API errors for individual PRs are logged as warnings and do not abort
// the poll; the loop continues to the next task.
func PollMergedPRs(ctx context.Context, ghClient gh.PRGetter, pool *pgxpool.Pool, workspaceID uuid.UUID) error {
	owner := "go"
	q := queries.New(pool)
	tasks, err := q.ListMergeablePRTasksForOwner(ctx, queries.ListMergeablePRTasksForOwnerParams{
		WorkspaceID: workspaceID,
		Owner:       &owner,
	})
	if err != nil {
		return fmt.Errorf("PollMergedPRs: list tasks: %w", err)
	}

	for _, task := range tasks {
		if err := processPRPoll(ctx, ghClient, pool, workspaceID, task); err != nil {
			log.Error().Err(err).
				Str("task_name", task.TaskName).
				Msg("PollMergedPRs: processing task failed")
		}
	}
	return nil
}

// processPRPoll handles a single task: checks GitHub and applies the appropriate
// transition based on the PR's merged/mergeable state.
func processPRPoll(
	ctx context.Context,
	ghClient gh.PRGetter,
	pool *pgxpool.Pool,
	workspaceID uuid.UUID,
	task queries.WorkspaceTask,
) error {
	prURL := extractPRURL(task.Pr)
	if prURL == "" {
		return nil
	}

	status, err := ghClient.GetPR(ctx, prURL)
	if err != nil {
		log.Warn().Err(err).
			Str("pr_url", prURL).
			Str("task_name", task.TaskName).
			Msg("GetPR failed; skipping task")
		return nil
	}

	// Merged-is-ground-truth: a merged PR transitions the task to done from any
	// of in_review/reviewing/review_passed (guarded by SetDoneFromMergedPR's WHERE).
	if status.Merged {
		ok, err := SetDoneFromMergedPR(ctx, pool, workspaceID, task.TaskID)
		if err != nil {
			return fmt.Errorf("SetDoneFromMergedPR: %w", err)
		}
		if ok {
			if err := AppendLog(ctx, pool, workspaceID, task.FeatureID, task.TaskID,
				"done", "orchestrator", "PR merged — task marked done."); err != nil {
				log.Warn().Err(err).
					Str("task_name", task.TaskName).
					Msg("AppendLog done failed; transition already committed")
			}
		}
		return nil
	}

	// Conflict detection: CONFLICTING → SetConflicted (skip if already resolving).
	// UNKNOWN → skip (GitHub not done computing; recheck next cycle).
	switch status.Mergeable {
	case "CONFLICTING":
		if task.ConflictState == ConflictStateResolving {
			// Rebase in-flight; do not interrupt.
			return nil
		}
		ok, err := SetConflicted(ctx, pool, workspaceID, task.TaskID)
		if err != nil {
			return fmt.Errorf("SetConflicted: %w", err)
		}
		if ok {
			log.Info().
				Str("task_name", task.TaskName).
				Str("pr_url", prURL).
				Msg("conflict detected — conflict_state set to conflicted")
		}
	case "UNKNOWN":
		// GitHub has not computed mergeability yet; skip and recheck next poll.
	}

	return nil
}

// extractPRURL parses the url field from the pr JSONB column.
// Returns an empty string if the JSON is malformed or the url key is absent/empty.
func extractPRURL(pr []byte) string {
	if len(pr) == 0 {
		return ""
	}
	var obj struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(pr, &obj); err != nil {
		return ""
	}
	return obj.URL
}
