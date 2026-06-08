package config_test

import (
	"os"
	"testing"

	"github.com/tiendv89/workflow-orchestrator/internal/config"
)

func setAllRequired(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("WORKSPACE_ID", "11111111-0000-0000-0000-000000000000")
	t.Setenv("ORGANIZATION_ID", "22222222-0000-0000-0000-000000000000")
	t.Setenv("BROKER_URL", "http://localhost:8080")
	t.Setenv("GITHUB_TOKEN", "ghp_test")
	t.Setenv("REDIS_URL", "localhost:6379")
	t.Setenv("MANAGEMENT_REPO", "tiendv89/project-workspace")
}

func TestLoad_MissingRequired(t *testing.T) {
	vars := []string{
		"DATABASE_URL", "WORKSPACE_ID", "ORGANIZATION_ID",
		"BROKER_URL", "GITHUB_TOKEN", "REDIS_URL", "MANAGEMENT_REPO",
	}
	for _, v := range vars {
		t.Setenv(v, "")
	}

	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error for missing required env vars, got nil")
	}
}

func TestLoad_AllPresent(t *testing.T) {
	setAllRequired(t)

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.DatabaseURL != "postgres://localhost/test" {
		t.Errorf("DatabaseURL = %q", cfg.DatabaseURL)
	}
	if cfg.PollIntervalSeconds != 15 {
		t.Errorf("PollIntervalSeconds default = %d, want 15", cfg.PollIntervalSeconds)
	}
	if cfg.BaseBranch != "main" {
		t.Errorf("BaseBranch default = %q, want main", cfg.BaseBranch)
	}
	if cfg.ManagementRepo != "tiendv89/project-workspace" {
		t.Errorf("ManagementRepo = %q", cfg.ManagementRepo)
	}
}

func TestLoad_BaseBranchOverride(t *testing.T) {
	setAllRequired(t)
	t.Setenv("BASE_BRANCH", "develop")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.BaseBranch != "develop" {
		t.Errorf("BaseBranch = %q, want develop", cfg.BaseBranch)
	}
}

func TestLoad_PollIntervalOverride(t *testing.T) {
	setAllRequired(t)
	t.Setenv("POLL_INTERVAL_SECONDS", "30")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.PollIntervalSeconds != 30 {
		t.Errorf("PollIntervalSeconds = %d, want 30", cfg.PollIntervalSeconds)
	}
}

func TestLoad_InvalidPollInterval(t *testing.T) {
	setAllRequired(t)
	t.Setenv("POLL_INTERVAL_SECONDS", "not-a-number")

	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error for invalid POLL_INTERVAL_SECONDS, got nil")
	}
}

func TestLoad_PartialMissing(t *testing.T) {
	os.Unsetenv("DATABASE_URL")
	os.Unsetenv("WORKSPACE_ID")
	t.Setenv("ORGANIZATION_ID", "22222222-0000-0000-0000-000000000000")
	t.Setenv("BROKER_URL", "http://localhost:8080")
	t.Setenv("GITHUB_TOKEN", "ghp_test")
	t.Setenv("REDIS_URL", "localhost:6379")
	t.Setenv("MANAGEMENT_REPO", "tiendv89/project-workspace")

	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error for partial missing env vars, got nil")
	}
}
