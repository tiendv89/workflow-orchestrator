package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
)

// Config holds all runtime configuration values for the orchestrator.
type Config struct {
	DatabaseURL         string
	WorkspaceID         string
	OrganizationID      string
	BrokerURL           string
	GitHubToken         string
	RedisURL            string
	ManagementRepo      string
	BaseBranch          string
	PollIntervalSeconds int
	HealthPort          int

	// Error/stuck recovery
	ExecutionDeadlineMS         int // max ms a task can be in_progress/reviewing; default 7200000 (2h)
	DispatchReconcileMaxRetries int // reconciler re-enqueue cap before block; default 3
	ExecutorMaxRetries          int // max-turns reset cap before block; default 3
}

// Load reads configuration from environment variables. Returns an error if any
// required variable is missing.
func Load() (*Config, error) {
	cfg := &Config{}
	var errs []error

	cfg.DatabaseURL = requireEnv("DATABASE_URL", &errs)
	cfg.WorkspaceID = requireEnv("WORKSPACE_ID", &errs)
	cfg.OrganizationID = requireEnv("ORGANIZATION_ID", &errs)
	cfg.BrokerURL = requireEnv("BROKER_URL", &errs)
	cfg.GitHubToken = requireEnv("GITHUB_TOKEN", &errs)
	cfg.RedisURL = requireEnv("REDIS_URL", &errs)
	cfg.ManagementRepo = requireEnv("MANAGEMENT_REPO", &errs)

	cfg.BaseBranch = os.Getenv("BASE_BRANCH")
	if cfg.BaseBranch == "" {
		cfg.BaseBranch = "main"
	}

	cfg.PollIntervalSeconds = 15
	if raw := os.Getenv("POLL_INTERVAL_SECONDS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			errs = append(errs, fmt.Errorf("POLL_INTERVAL_SECONDS must be an integer: %w", err))
		} else {
			cfg.PollIntervalSeconds = n
		}
	}

	cfg.HealthPort = 8080
	if raw := os.Getenv("HEALTH_PORT"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			errs = append(errs, fmt.Errorf("HEALTH_PORT must be an integer: %w", err))
		} else {
			cfg.HealthPort = n
		}
	}

	cfg.ExecutionDeadlineMS = 7200000
	if raw := os.Getenv("EXECUTION_DEADLINE_MS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			errs = append(errs, fmt.Errorf("EXECUTION_DEADLINE_MS must be an integer: %w", err))
		} else {
			cfg.ExecutionDeadlineMS = n
		}
	}

	cfg.DispatchReconcileMaxRetries = 3
	if raw := os.Getenv("DISPATCH_RECONCILE_MAX_RETRIES"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			errs = append(errs, fmt.Errorf("DISPATCH_RECONCILE_MAX_RETRIES must be an integer: %w", err))
		} else {
			cfg.DispatchReconcileMaxRetries = n
		}
	}

	cfg.ExecutorMaxRetries = 3
	if raw := os.Getenv("EXECUTOR_MAX_RETRIES"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			errs = append(errs, fmt.Errorf("EXECUTOR_MAX_RETRIES must be an integer: %w", err))
		} else {
			cfg.ExecutorMaxRetries = n
		}
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return cfg, nil
}

func requireEnv(key string, errs *[]error) string {
	v := os.Getenv(key)
	if v == "" {
		*errs = append(*errs, fmt.Errorf("required environment variable %s is not set", key))
	}
	return v
}
