package config_test

import (
	"os"
	"testing"

	"github.com/tiendv89/workflow-orchestrator/internal/config"
)

func TestLoad_MissingRequired(t *testing.T) {
	// Clear all required env vars.
	vars := []string{"DATABASE_URL", "WORKSPACE_ID", "ORGANIZATION_ID", "BROKER_URL", "GITHUB_TOKEN"}
	for _, v := range vars {
		t.Setenv(v, "")
	}

	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error for missing required env vars, got nil")
	}
}

func TestLoad_AllPresent(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("WORKSPACE_ID", "11111111-0000-0000-0000-000000000000")
	t.Setenv("ORGANIZATION_ID", "22222222-0000-0000-0000-000000000000")
	t.Setenv("BROKER_URL", "http://localhost:8080")
	t.Setenv("GITHUB_TOKEN", "ghp_test")

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
}

func TestLoad_PollIntervalOverride(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("WORKSPACE_ID", "11111111-0000-0000-0000-000000000000")
	t.Setenv("ORGANIZATION_ID", "22222222-0000-0000-0000-000000000000")
	t.Setenv("BROKER_URL", "http://localhost:8080")
	t.Setenv("GITHUB_TOKEN", "ghp_test")
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
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("WORKSPACE_ID", "11111111-0000-0000-0000-000000000000")
	t.Setenv("ORGANIZATION_ID", "22222222-0000-0000-0000-000000000000")
	t.Setenv("BROKER_URL", "http://localhost:8080")
	t.Setenv("GITHUB_TOKEN", "ghp_test")
	t.Setenv("POLL_INTERVAL_SECONDS", "not-a-number")

	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error for invalid POLL_INTERVAL_SECONDS, got nil")
	}
}

func TestLoad_PartialMissing(t *testing.T) {
	// Only set some vars; others are missing.
	os.Unsetenv("DATABASE_URL")
	os.Unsetenv("WORKSPACE_ID")
	t.Setenv("ORGANIZATION_ID", "22222222-0000-0000-0000-000000000000")
	t.Setenv("BROKER_URL", "http://localhost:8080")
	t.Setenv("GITHUB_TOKEN", "ghp_test")

	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error for partial missing env vars, got nil")
	}
}
