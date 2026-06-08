package orchestrator_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tiendv89/workflow-orchestrator/internal/database/queries"
	"github.com/tiendv89/workflow-orchestrator/internal/orchestrator"
)

// insertNamedTask inserts a task with a specific name, status, and depends_on list.
// Returns the task's task_id UUID.
func insertNamedTask(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	fx testFixture,
	taskName, status string,
	dependsOn []string,
) uuid.UUID {
	t.Helper()
	q := queries.New(pool)
	taskID := uuid.New()
	repo := "test-repo"
	owner := "go"

	depJSON, err := json.Marshal(dependsOn)
	if err != nil {
		t.Fatalf("insertNamedTask: marshal depends_on: %v", err)
	}

	_, err = q.InsertTask(ctx, queries.InsertTaskParams{
		WorkspaceID: fx.workspaceID,
		FeatureID:   fx.featureID,
		FeatureName: "test-feature",
		TaskID:      taskID,
		TaskName:    taskName,
		Title:       "Task " + taskName,
		Repo:        &repo,
		Status:      &status,
		DependsOn:   depJSON,
		Owner:       &owner,
	})
	if err != nil {
		t.Fatalf("insertNamedTask %q: %v", taskName, err)
	}
	return taskID
}

// getTaskStatus fetches the current status of a task by its task_id UUID.
func getTaskStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fx testFixture, taskID uuid.UUID) string {
	t.Helper()
	row := getTaskRow(t, ctx, pool, fx, taskID)
	if row.Status == nil {
		return ""
	}
	return *row.Status
}

// TestAutoReadyDependents_DiamondDependency verifies the canonical diamond pattern:
//
//	T_base → {T_left, T_right} → T_tip
//
// Mark T_base done → T_left and T_right advance to ready, T_tip stays todo.
// Mark T_left and T_right done → T_tip advances to ready.
func TestAutoReadyDependents_DiamondDependency(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	// Seed: T_base (in_review), T_left and T_right (todo, depend on T_base),
	// T_tip (todo, depends on both T_left and T_right).
	baseID := insertNamedTask(t, ctx, pool, fx, "T_base", "in_review", []string{})
	leftID := insertNamedTask(t, ctx, pool, fx, "T_left", "todo", []string{"T_base"})
	rightID := insertNamedTask(t, ctx, pool, fx, "T_right", "todo", []string{"T_base"})
	tipID := insertNamedTask(t, ctx, pool, fx, "T_tip", "todo", []string{"T_left", "T_right"})

	// Mark T_base done — this triggers AutoReadyDependents via SetDone.
	ok, err := orchestrator.SetDone(ctx, pool, fx.workspaceID, baseID)
	if err != nil {
		t.Fatalf("SetDone(T_base): %v", err)
	}
	if !ok {
		t.Fatal("SetDone(T_base): expected ok=true")
	}

	// T_left and T_right should now be ready.
	if s := getTaskStatus(t, ctx, pool, fx, leftID); s != "ready" {
		t.Errorf("T_left: expected status=ready after T_base done, got %q", s)
	}
	if s := getTaskStatus(t, ctx, pool, fx, rightID); s != "ready" {
		t.Errorf("T_right: expected status=ready after T_base done, got %q", s)
	}
	// T_tip still has unmet deps — must remain todo.
	if s := getTaskStatus(t, ctx, pool, fx, tipID); s != "todo" {
		t.Errorf("T_tip: expected status=todo (unmet deps), got %q", s)
	}

	// Transition T_left and T_right through to done.
	for _, id := range []uuid.UUID{leftID, rightID} {
		// ready → in_progress → in_review → done
		ok, err = orchestrator.GuardedTransition(ctx, pool, fx.workspaceID, id, "ready", "in_progress", nil)
		if err != nil || !ok {
			t.Fatalf("ready→in_progress failed: ok=%v err=%v", ok, err)
		}
		ok, err = orchestrator.SetInReview(ctx, pool, fx.workspaceID, id, "https://github.com/example/pr/test")
		if err != nil || !ok {
			t.Fatalf("in_progress→in_review failed: ok=%v err=%v", ok, err)
		}
	}

	// Mark T_left done — T_tip still has T_right pending.
	ok, err = orchestrator.SetDone(ctx, pool, fx.workspaceID, leftID)
	if err != nil {
		t.Fatalf("SetDone(T_left): %v", err)
	}
	if !ok {
		t.Fatal("SetDone(T_left): expected ok=true")
	}
	if s := getTaskStatus(t, ctx, pool, fx, tipID); s != "todo" {
		t.Errorf("T_tip: expected status=todo after only T_left done, got %q", s)
	}

	// Mark T_right done — T_tip should now advance to ready.
	ok, err = orchestrator.SetDone(ctx, pool, fx.workspaceID, rightID)
	if err != nil {
		t.Fatalf("SetDone(T_right): %v", err)
	}
	if !ok {
		t.Fatal("SetDone(T_right): expected ok=true")
	}
	if s := getTaskStatus(t, ctx, pool, fx, tipID); s != "ready" {
		t.Errorf("T_tip: expected status=ready after both deps done, got %q", s)
	}
}

// TestAutoReadyDependents_NoQualifyingTask verifies that calling AutoReadyDependents
// when no todo task has all its dependencies met returns an empty slice without error.
func TestAutoReadyDependents_NoQualifyingTask(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	// T_dep depends on T_done_name, but T_dep's other dep (T_other) is not done.
	_ = insertNamedTask(t, ctx, pool, fx, "T_done", "done", []string{})
	_ = insertNamedTask(t, ctx, pool, fx, "T_other", "in_progress", []string{})
	depID := insertNamedTask(t, ctx, pool, fx, "T_dep", "todo", []string{"T_done", "T_other"})

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	advanced, err := orchestrator.AutoReadyDependents(ctx, tx, fx.workspaceID, "T_done")
	if err != nil {
		t.Fatalf("AutoReadyDependents: %v", err)
	}
	if len(advanced) != 0 {
		t.Errorf("expected empty slice, got %v", advanced)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// T_dep must still be todo — it has an unmet dependency.
	if s := getTaskStatus(t, ctx, pool, fx, depID); s != "todo" {
		t.Errorf("T_dep: expected status=todo, got %q", s)
	}
}

// TestAutoReadyDependents_Rollback verifies that if the enclosing transaction is
// rolled back, the dependent-advance is also rolled back.
func TestAutoReadyDependents_Rollback(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	// T_base is being marked done; T_dep depends on it.
	baseID := insertNamedTask(t, ctx, pool, fx, "T_base_rb", "in_review", []string{})
	depID := insertNamedTask(t, ctx, pool, fx, "T_dep_rb", "todo", []string{"T_base_rb"})
	_ = baseID

	// Begin a transaction, update T_base to done, and call AutoReadyDependents.
	// Then rollback — both the done-update and the ready-advance must be undone.
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	// Manually update T_base to done within the tx.
	_, err = tx.Exec(ctx, `
		UPDATE workspace_tasks SET status='done', updated_at=now()
		WHERE workspace_id=$1 AND task_id=$2`,
		fx.workspaceID, baseID,
	)
	if err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		t.Fatalf("update T_base to done: %v", err)
	}

	advanced, err := orchestrator.AutoReadyDependents(ctx, tx, fx.workspaceID, "T_base_rb")
	if err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		t.Fatalf("AutoReadyDependents: %v", err)
	}
	if len(advanced) != 1 {
		tx.Rollback(ctx) //nolint:errcheck
		t.Fatalf("expected 1 advanced task within tx, got %d", len(advanced))
	}

	// Rollback — neither update should persist.
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// Both tasks must be in their original statuses.
	if s := getTaskStatus(t, ctx, pool, fx, baseID); s != "in_review" {
		t.Errorf("T_base: expected status=in_review after rollback, got %q", s)
	}
	if s := getTaskStatus(t, ctx, pool, fx, depID); s != "todo" {
		t.Errorf("T_dep: expected status=todo after rollback, got %q", s)
	}
}

// TestSetDone_WrongPrecondition_DoesNotAdvanceDependents verifies that when
// SetDone fails because the task is not in "in_review", no dependents advance.
func TestSetDone_WrongPrecondition_DoesNotAdvanceDependents(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	fx := setupFixture(t, ctx, pool)

	// T_source is in_progress (not in_review) — SetDone precondition will fail.
	sourceID := insertNamedTask(t, ctx, pool, fx, "T_src_wp", "in_progress", []string{})
	depID := insertNamedTask(t, ctx, pool, fx, "T_dep_wp", "todo", []string{"T_src_wp"})

	ok, err := orchestrator.SetDone(ctx, pool, fx.workspaceID, sourceID)
	if err != nil {
		t.Fatalf("SetDone returned unexpected error: %v", err)
	}
	if ok {
		t.Error("expected ok=false for wrong precondition")
	}

	// Dependent must remain todo.
	if s := getTaskStatus(t, ctx, pool, fx, depID); s != "todo" {
		t.Errorf("T_dep: expected status=todo, got %q", s)
	}
}
