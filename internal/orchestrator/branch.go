package orchestrator

import (
	"fmt"

	db "github.com/tiendv89/workflow-orchestrator/internal/database/queries"
)

// FeatureBranchName returns the canonical feature branch name.
// Matches agent-workflow/runtime/orchestrator/src/paths.ts featureBranchName().
func FeatureBranchName(featureName string) string {
	return "feature/" + featureName
}

// TaskBranchName returns the canonical implementation-repo branch for a task.
// Matches agent-workflow/runtime/orchestrator/src/paths.ts taskBranchName().
func TaskBranchName(featureName, taskName string) string {
	return fmt.Sprintf("feature/%s-%s", featureName, taskName)
}

// ResolveTaskBranch returns the branch to pass to the executor dispatch job.
// Uses the persisted branch when set; otherwise derives the canonical name.
func ResolveTaskBranch(task db.WorkspaceTask) string {
	if task.Branch != nil && *task.Branch != "" {
		return *task.Branch
	}
	return TaskBranchName(task.FeatureName, task.TaskName)
}
