package orchestrator

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tiendv89/workflow-orchestrator/internal/database/queries"
)

// AppendLog inserts a task-scoped activity event into workspace_activity_events.
// The sequence number is computed as COALESCE(MAX(sequence), 0) + 1 within a
// transaction to prevent duplicate sequence collisions under concurrent writes.
func AppendLog(
	ctx context.Context,
	pool *pgxpool.Pool,
	workspaceID, featureUUID, taskUUID uuid.UUID,
	action, by, note string,
) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("AppendLog: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	q := queries.New(tx)
	featurePg := pgtype.UUID{Bytes: featureUUID, Valid: true}
	taskPg := pgtype.UUID{Bytes: taskUUID, Valid: true}

	seq, err := q.GetNextActivitySequence(ctx, queries.GetNextActivitySequenceParams{
		WorkspaceID: workspaceID,
		FeatureID:   featurePg,
		TaskID:      taskPg,
	})
	if err != nil {
		return fmt.Errorf("AppendLog: get next sequence: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	scopeType := "task"
	act := action
	actor := by
	n := note

	_, err = q.InsertActivityEvent(ctx, queries.InsertActivityEventParams{
		WorkspaceID: workspaceID,
		ScopeType:   scopeType,
		FeatureID:   featurePg,
		FeatureName: nil,
		TaskID:      taskPg,
		TaskName:    nil,
		Action:      &act,
		Actor:       &actor,
		OccurredAt:  &now,
		Note:        &n,
		Sequence:    seq,
		RawEvent:    []byte("{}"),
	})
	if err != nil {
		return fmt.Errorf("AppendLog: insert event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("AppendLog: commit: %w", err)
	}
	return nil
}
