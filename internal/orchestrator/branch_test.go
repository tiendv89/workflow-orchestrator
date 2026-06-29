package orchestrator

import (
	"testing"

	db "github.com/tiendv89/workflow-orchestrator/internal/database/queries"
)

func TestTaskBranchName(t *testing.T) {
	got := TaskBranchName("my-feature", "T11")
	want := "feature/my-feature-T11"
	if got != want {
		t.Errorf("TaskBranchName() = %q, want %q", got, want)
	}
}

func TestResolveTaskBranch_PrefersPersisted(t *testing.T) {
	persisted := "feature/custom-branch"
	task := db.WorkspaceTask{
		FeatureName: "my-feature",
		TaskName:    "T11",
		Branch:      &persisted,
	}
	if got := ResolveTaskBranch(task); got != persisted {
		t.Errorf("ResolveTaskBranch() = %q, want %q", got, persisted)
	}
}

func TestResolveTaskBranch_DerivesWhenMissing(t *testing.T) {
	task := db.WorkspaceTask{
		FeatureName: "my-feature",
		TaskName:    "T11",
	}
	want := "feature/my-feature-T11"
	if got := ResolveTaskBranch(task); got != want {
		t.Errorf("ResolveTaskBranch() = %q, want %q", got, want)
	}
}
