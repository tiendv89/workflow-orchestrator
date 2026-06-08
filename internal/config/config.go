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
	ImplRepoURL         string
	PollIntervalSeconds int
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

	cfg.ImplRepoURL = os.Getenv("IMPL_REPO_URL")

	cfg.PollIntervalSeconds = 15
	if raw := os.Getenv("POLL_INTERVAL_SECONDS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			errs = append(errs, fmt.Errorf("POLL_INTERVAL_SECONDS must be an integer: %w", err))
		} else {
			cfg.PollIntervalSeconds = n
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
