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
	if cfg.HealthPort != 8080 {
		t.Errorf("HealthPort default = %d, want 8080", cfg.HealthPort)
	}
	if cfg.BaseBranch != "main" {
		t.Errorf("BaseBranch default = %q, want main", cfg.BaseBranch)
	}
	if cfg.ManagementRepo != "tiendv89/project-workspace" {
		t.Errorf("ManagementRepo = %q", cfg.ManagementRepo)
	}
	// error/stuck recovery defaults
	if cfg.ExecutionDeadlineMS != 7200000 {
		t.Errorf("ExecutionDeadlineMS default = %d, want 7200000", cfg.ExecutionDeadlineMS)
	}
	if cfg.DispatchReconcileMaxRetries != 3 {
		t.Errorf("DispatchReconcileMaxRetries default = %d, want 3", cfg.DispatchReconcileMaxRetries)
	}
	if cfg.ExecutorMaxRetries != 3 {
		t.Errorf("ExecutorMaxRetries default = %d, want 3", cfg.ExecutorMaxRetries)
	}
	// conflict resolution defaults
	if cfg.MaxRebaseAttempts != 3 {
		t.Errorf("MaxRebaseAttempts default = %d, want 3", cfg.MaxRebaseAttempts)
	}
	// soft-claim throttle defaults
	if cfg.MaxInFlight != 5 {
		t.Errorf("MaxInFlight default = %d, want 5", cfg.MaxInFlight)
	}
}

func TestLoad_RecoveryConfigOverrides(t *testing.T) {
	setAllRequired(t)
	t.Setenv("EXECUTION_DEADLINE_MS", "3600000")
	t.Setenv("DISPATCH_RECONCILE_MAX_RETRIES", "5")
	t.Setenv("EXECUTOR_MAX_RETRIES", "2")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ExecutionDeadlineMS != 3600000 {
		t.Errorf("ExecutionDeadlineMS = %d, want 3600000", cfg.ExecutionDeadlineMS)
	}
	if cfg.DispatchReconcileMaxRetries != 5 {
		t.Errorf("DispatchReconcileMaxRetries = %d, want 5", cfg.DispatchReconcileMaxRetries)
	}
	if cfg.ExecutorMaxRetries != 2 {
		t.Errorf("ExecutorMaxRetries = %d, want 2", cfg.ExecutorMaxRetries)
	}
}

func TestLoad_InvalidRecoveryConfig(t *testing.T) {
	setAllRequired(t)
	t.Setenv("EXECUTION_DEADLINE_MS", "not-a-number")

	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error for invalid EXECUTION_DEADLINE_MS, got nil")
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

func TestLoad_HealthPortOverride(t *testing.T) {
	setAllRequired(t)
	t.Setenv("HEALTH_PORT", "9090")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.HealthPort != 9090 {
		t.Errorf("HealthPort = %d, want 9090", cfg.HealthPort)
	}
}

func TestLoad_InvalidHealthPort(t *testing.T) {
	setAllRequired(t)
	t.Setenv("HEALTH_PORT", "not-a-number")

	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error for invalid HEALTH_PORT, got nil")
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
	_ = os.Unsetenv("DATABASE_URL")
	_ = os.Unsetenv("WORKSPACE_ID")
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

func TestLoad_MaxRebaseAttemptsOverride(t *testing.T) {
	setAllRequired(t)
	t.Setenv("MAX_REBASE_ATTEMPTS", "5")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MaxRebaseAttempts != 5 {
		t.Errorf("MaxRebaseAttempts = %d, want 5", cfg.MaxRebaseAttempts)
	}
}

func TestLoad_InvalidMaxRebaseAttempts(t *testing.T) {
	setAllRequired(t)
	t.Setenv("MAX_REBASE_ATTEMPTS", "not-a-number")

	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error for invalid MAX_REBASE_ATTEMPTS, got nil")
	}
}

func TestLoad_MaxInFlightDefault(t *testing.T) {
	setAllRequired(t)

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MaxInFlight != 5 {
		t.Errorf("MaxInFlight default = %d, want 5", cfg.MaxInFlight)
	}
}

func TestLoad_MaxInFlightOverride(t *testing.T) {
	setAllRequired(t)
	t.Setenv("MAX_INFLIGHT", "10")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MaxInFlight != 10 {
		t.Errorf("MaxInFlight = %d, want 10", cfg.MaxInFlight)
	}
}

func TestLoad_InvalidMaxInFlight(t *testing.T) {
	setAllRequired(t)
	t.Setenv("MAX_INFLIGHT", "not-a-number")

	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error for invalid MAX_INFLIGHT, got nil")
	}
}
