package orchestrator_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/tiendv89/workflow-orchestrator/internal/database/queries"
	"github.com/tiendv89/workflow-orchestrator/internal/orchestrator"
)

func TestAppendLog_InsertsRow(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_progress")

	if err := orchestrator.AppendLog(ctx, pool, fx.workspaceID, fx.featureID, taskID, "started", "executor@test.com", "test note"); err != nil {
		t.Fatalf("AppendLog: %v", err)
	}

	// After one insert, next sequence should be 2.
	q := queries.New(pool)
	seq, err := q.GetNextActivitySequence(ctx, queries.GetNextActivitySequenceParams{
		WorkspaceID: fx.workspaceID,
		FeatureID:   pgtype.UUID{Bytes: fx.featureID, Valid: true},
		TaskID:      pgtype.UUID{Bytes: taskID, Valid: true},
	})
	if err != nil {
		t.Fatalf("GetNextActivitySequence: %v", err)
	}
	if seq != 2 {
		t.Errorf("expected next sequence=2 after one insert, got %d", seq)
	}
}

func TestAppendLog_SequenceIncrements(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID := insertTaskWithStatus(t, ctx, pool, fx, "in_progress")

	for i := range 3 {
		if err := orchestrator.AppendLog(ctx, pool, fx.workspaceID, fx.featureID, taskID, "update", "exec", "note"); err != nil {
			t.Fatalf("AppendLog iteration %d: %v", i, err)
		}
	}

	// After 3 inserts, next sequence should be 4.
	q := queries.New(pool)
	seq, err := q.GetNextActivitySequence(ctx, queries.GetNextActivitySequenceParams{
		WorkspaceID: fx.workspaceID,
		FeatureID:   pgtype.UUID{Bytes: fx.featureID, Valid: true},
		TaskID:      pgtype.UUID{Bytes: taskID, Valid: true},
	})
	if err != nil {
		t.Fatalf("GetNextActivitySequence: %v", err)
	}
	if seq != 4 {
		t.Errorf("expected next sequence=4 after 3 inserts, got %d", seq)
	}
}

func TestAppendLog_SequenceIsolatedByTask(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)
	taskID1 := insertTaskWithStatus(t, ctx, pool, fx, "in_progress")
	taskID2 := insertTaskWithStatus(t, ctx, pool, fx, "in_progress")

	// 2 events for task1, 1 for task2.
	for i := range 2 {
		if err := orchestrator.AppendLog(ctx, pool, fx.workspaceID, fx.featureID, taskID1, "update", "exec", "note"); err != nil {
			t.Fatalf("AppendLog task1 i=%d: %v", i, err)
		}
	}
	if err := orchestrator.AppendLog(ctx, pool, fx.workspaceID, fx.featureID, taskID2, "update", "exec", "note"); err != nil {
		t.Fatalf("AppendLog task2: %v", err)
	}

	q := queries.New(pool)
	featurePg := pgtype.UUID{Bytes: fx.featureID, Valid: true}

	seq1, err := q.GetNextActivitySequence(ctx, queries.GetNextActivitySequenceParams{
		WorkspaceID: fx.workspaceID,
		FeatureID:   featurePg,
		TaskID:      pgtype.UUID{Bytes: taskID1, Valid: true},
	})
	if err != nil {
		t.Fatalf("GetNextActivitySequence task1: %v", err)
	}
	if seq1 != 3 {
		t.Errorf("expected task1 next_seq=3, got %d", seq1)
	}

	seq2, err := q.GetNextActivitySequence(ctx, queries.GetNextActivitySequenceParams{
		WorkspaceID: fx.workspaceID,
		FeatureID:   featurePg,
		TaskID:      pgtype.UUID{Bytes: taskID2, Valid: true},
	})
	if err != nil {
		t.Fatalf("GetNextActivitySequence task2: %v", err)
	}
	if seq2 != 2 {
		t.Errorf("expected task2 next_seq=2, got %d", seq2)
	}
}
