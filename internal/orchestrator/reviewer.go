package orchestrator

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/tiendv89/workflow-orchestrator/internal/config"
	db "github.com/tiendv89/workflow-orchestrator/internal/database/queries"
)

// MaxReviewIncompletes is the maximum number of consecutive no-valid-verdict
// outcomes before a task is escalated from review_incomplete to blocked.
const MaxReviewIncompletes = 2

// FindReviewableTasks returns go-owned tasks in in_review or review_incomplete
// status that have a PR URL set and are eligible for reviewer dispatch.
func FindReviewableTasks(ctx context.Context, pool *pgxpool.Pool, workspaceID uuid.UUID) ([]db.WorkspaceTask, error) {
	q := db.New(pool)
	owner := "go"
	tasks, err := q.ListReviewableTasksForOwner(ctx, db.ListReviewableTasksForOwnerParams{
		WorkspaceID: workspaceID,
		Owner:       &owner,
	})
	if err != nil {
		return nil, fmt.Errorf("FindReviewableTasks: %w", err)
	}
	return tasks, nil
}

// DispatchReviewer claims a reviewable task (in_review or review_incomplete)
// for a reviewer agent (guarded in_review→reviewing or
// review_incomplete→reviewing). First-push-wins: returns (false, nil) if
// another instance won the claim.
//
// On claim success, registers the handle in the HandleStore and dispatches a
// broker "review" job. If broker dispatch fails, the reviewer claim is rolled
// back to the original fromStatus.
func DispatchReviewer(
	ctx context.Context,
	pool *pgxpool.Pool,
	cfg *config.Config,
	workspaceID uuid.UUID,
	task db.WorkspaceTask,
	dispatcher *Dispatcher,
	hs *HandleStore,
) (bool, error) {
	fromStatus := taskStatus(task)
	handle := uuid.New().String()
	nonce := uuid.New().String()

	// Guarded claim: fromStatus → reviewing (first-push-wins).
	won, err := SetReviewing(ctx, pool, workspaceID, task.TaskID, fromStatus, handle, nonce)
	if err != nil {
		return false, fmt.Errorf("DispatchReviewer: SetReviewing: %w", err)
	}
	if !won {
		return false, nil
	}

	// Dispatch broker review job. On failure, roll back the claim.
	if dispatchErr := dispatcher.DispatchWithNonce(ctx, cfg, task, handle, nonce, "review"); dispatchErr != nil {
		if _, rbErr := GuardedTransition(ctx, pool, workspaceID, task.TaskID, "reviewing", fromStatus, dispatchOutExtra()); rbErr != nil {
			log.Error().Err(rbErr).
				Str("task", task.TaskName).
				Str("from_status", fromStatus).
				Msg("DispatchReviewer: rollback failed — task may be stuck in reviewing")
		}
		return false, fmt.Errorf("DispatchReviewer: dispatch: %w", dispatchErr)
	}

	hs.Register(handle, HandleEntry{
		FeatureUUID: task.FeatureID,
		TaskUUID:    task.TaskID,
		FeatureName: task.FeatureName,
		TaskName:    task.TaskName,
	})
	return true, nil
}

// FindFixableTasks returns go-owned tasks in change_requested status that have
// a PR URL set and are eligible for a fix agent dispatch.
func FindFixableTasks(ctx context.Context, pool *pgxpool.Pool, workspaceID uuid.UUID) ([]db.WorkspaceTask, error) {
	q := db.New(pool)
	owner := "go"
	tasks, err := q.ListChangeRequestedTasksForOwner(ctx, db.ListChangeRequestedTasksForOwnerParams{
		WorkspaceID: workspaceID,
		Owner:       &owner,
	})
	if err != nil {
		return nil, fmt.Errorf("FindFixableTasks: %w", err)
	}
	return tasks, nil
}

// DispatchFix claims a change_requested task for a fix agent (guarded
// change_requested→in_progress). First-push-wins: returns (false, nil) if
// another instance won the claim.
//
// The fix agent uses broker kind "fix" (runs respond-to-review) and is
// expected to complete with terminal_status "in_review" (re-opens the review
// cycle). On broker dispatch failure the claim is rolled back to
// change_requested.
func DispatchFix(
	ctx context.Context,
	pool *pgxpool.Pool,
	cfg *config.Config,
	workspaceID uuid.UUID,
	task db.WorkspaceTask,
	dispatcher *Dispatcher,
	hs *HandleStore,
) (bool, error) {
	handle := uuid.New().String()
	nonce := uuid.New().String()

	// Guarded claim: change_requested → in_progress with dispatch-in columns.
	won, err := ClaimFix(ctx, pool, workspaceID, task.TaskID, handle, nonce)
	if err != nil {
		return false, fmt.Errorf("DispatchFix: ClaimFix: %w", err)
	}
	if !won {
		return false, nil
	}

	// Dispatch broker fix job. On failure, roll back the claim.
	if dispatchErr := dispatcher.DispatchWithNonce(ctx, cfg, task, handle, nonce, "fix"); dispatchErr != nil {
		if _, rbErr := GuardedTransition(ctx, pool, workspaceID, task.TaskID, "in_progress", "change_requested", dispatchOutExtra()); rbErr != nil {
			log.Error().Err(rbErr).
				Str("task", task.TaskName).
				Msg("DispatchFix: rollback failed — task may be stuck in in_progress")
		}
		return false, fmt.Errorf("DispatchFix: dispatch: %w", dispatchErr)
	}

	hs.Register(handle, HandleEntry{
		FeatureUUID: task.FeatureID,
		TaskUUID:    task.TaskID,
		FeatureName: task.FeatureName,
		TaskName:    task.TaskName,
	})
	return true, nil
}

// taskStatus safely dereferences the nullable status pointer from a WorkspaceTask row.
func taskStatus(task db.WorkspaceTask) string {
	if task.Status != nil {
		return *task.Status
	}
	return ""
}
