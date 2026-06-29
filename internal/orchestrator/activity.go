package orchestrator

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tiendv89/workflow-orchestrator/internal/database/queries"
)

// advisoryKey maps a (workspace, feature, task) triple to a stable int64 advisory lock key.
func advisoryKey(workspaceID, featureUUID, taskUUID uuid.UUID) int64 {
	h := fnv.New64a()
	h.Write(workspaceID[:])
	h.Write(featureUUID[:])
	h.Write(taskUUID[:])
	return int64(h.Sum64())
}

// appendLogInsert performs the sequence-fetch + insert within an already-open transaction.
// An advisory lock is acquired first to serialize concurrent appends for the same task.
func appendLogInsert(
	ctx context.Context,
	tx pgx.Tx,
	workspaceID, featureUUID, taskUUID uuid.UUID,
	action, by, note string,
) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, advisoryKey(workspaceID, featureUUID, taskUUID)); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}

	featurePg := pgtype.UUID{Bytes: featureUUID, Valid: true}
	taskPg := pgtype.UUID{Bytes: taskUUID, Valid: true}
	q := queries.New(tx)

	seq, err := q.GetNextActivitySequence(ctx, queries.GetNextActivitySequenceParams{
		WorkspaceID: workspaceID,
		FeatureID:   featurePg,
		TaskID:      taskPg,
	})
	if err != nil {
		return fmt.Errorf("get next sequence: %w", err)
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
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

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

	if err := appendLogInsert(ctx, tx, workspaceID, featureUUID, taskUUID, action, by, note); err != nil {
		return fmt.Errorf("AppendLog: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("AppendLog: commit: %w", err)
	}
	return nil
}

// AppendLogTx inserts a task-scoped activity event within an existing transaction.
// The caller is responsible for committing or rolling back the transaction.
func AppendLogTx(
	ctx context.Context,
	tx pgx.Tx,
	workspaceID, featureUUID, taskUUID uuid.UUID,
	action, by, note string,
) error {
	if err := appendLogInsert(ctx, tx, workspaceID, featureUUID, taskUUID, action, by, note); err != nil {
		return fmt.Errorf("AppendLogTx: %w", err)
	}
	return nil
}
