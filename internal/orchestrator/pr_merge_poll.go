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

// PollMergedPRs scans all go-owned tasks in "in_review" status that have a PR
// URL stored in the pr JSONB field. For each task, it checks the GitHub API to
// see if the PR has been merged. If merged, it transitions the task to "done"
// and auto-readies any dependents — all within a single DB transaction. A
// "done" activity log entry is appended after the transaction commits.
//
// GitHub API errors for individual PRs are logged as warnings and do not abort
// the poll; the loop continues to the next task.
func PollMergedPRs(ctx context.Context, ghClient gh.PRGetter, pool *pgxpool.Pool, workspaceID uuid.UUID) error {
	owner := "go"
	q := queries.New(pool)
	tasks, err := q.ListInReviewTasksForOwner(ctx, queries.ListInReviewTasksForOwnerParams{
		WorkspaceID: workspaceID,
		Owner:       &owner,
	})
	if err != nil {
		return fmt.Errorf("PollMergedPRs: list tasks: %w", err)
	}

	for _, task := range tasks {
		if err := processMergedPR(ctx, ghClient, pool, workspaceID, task); err != nil {
			log.Error().Err(err).
				Str("task_name", task.TaskName).
				Msg("PollMergedPRs: processing task failed")
		}
	}
	return nil
}

// processMergedPR handles a single in_review task: checks GitHub and, if the
// PR is merged, applies the done transition + auto-ready in one transaction.
func processMergedPR(
	ctx context.Context,
	ghClient gh.PRGetter,
	pool *pgxpool.Pool,
	workspaceID uuid.UUID,
	task queries.WorkspaceTask,
) error {
	prURL := extractPRURL(task.Pr)
	if prURL == "" {
		return nil // no URL stored yet — skip silently
	}

	status, err := ghClient.GetPR(ctx, prURL)
	if err != nil {
		log.Warn().Err(err).
			Str("pr_url", prURL).
			Str("task_name", task.TaskName).
			Msg("GetPR failed; skipping task")
		return nil // non-fatal: continue to next task
	}
	if !status.Merged {
		return nil // PR still open — nothing to do
	}

	// SetDone transitions in_review → done and auto-readies dependents in one tx.
	ok, err := SetDone(ctx, pool, workspaceID, task.TaskID)
	if err != nil {
		return fmt.Errorf("SetDone: %w", err)
	}
	if ok {
		// Append done activity log entry only when the transition was applied.
		if err := AppendLog(ctx, pool, workspaceID, task.FeatureID, task.TaskID,
			"done", "orchestrator", "PR merged — task marked done."); err != nil {
			log.Warn().Err(err).
				Str("task_name", task.TaskName).
				Msg("AppendLog done failed; transition already committed")
		}
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
