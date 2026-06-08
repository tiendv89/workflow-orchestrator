package orchestrator_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/tiendv89/workflow-orchestrator/internal/database"
)

// TestMain applies the real goose migrations before any test in this package
// runs, so that unit tests that connect to DATABASE_URL find the schema ready.
//
// If DATABASE_URL is unset, migrations are skipped — individual tests that
// need a database will skip themselves via openPool.
func TestMain(m *testing.M) {
	os.Exit(runSuite(m))
}

func runSuite(m *testing.M) int {
	dsn := os.Getenv("DATABASE_URL")
	if dsn != "" {
		if err := applyMigrations(context.Background(), dsn); err != nil {
			fmt.Fprintf(os.Stderr, "orchestrator tests: apply migrations: %v\n", err)
			return 1
		}
	}
	return m.Run()
}

func applyMigrations(ctx context.Context, dsn string) error {
	pool, err := database.Open(ctx, dsn)
	if err != nil {
		return fmt.Errorf("open pool: %w", err)
	}
	defer database.Close(pool)

	// Resolve the migration directory relative to this package's location.
	// The package lives at internal/orchestrator/; migrations are at db/migrations/.
	const migrationsDir = "../../db/migrations"

	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("read migrations dir %s: %w", migrationsDir, err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		raw, err := os.ReadFile(migrationsDir + "/" + entry.Name())
		if err != nil {
			return fmt.Errorf("read %s: %w", entry.Name(), err)
		}
		upSQL := extractUpSection(string(raw))
		if upSQL == "" {
			continue
		}
		if _, err := pool.Exec(ctx, upSQL); err != nil {
			return fmt.Errorf("apply %s: %w", entry.Name(), err)
		}
	}
	return nil
}

func extractUpSection(content string) string {
	const (
		upMarker   = "-- +goose Up"
		downMarker = "-- +goose Down"
	)
	upIdx := strings.Index(content, upMarker)
	if upIdx == -1 {
		return ""
	}
	after := content[upIdx+len(upMarker):]
	if downIdx := strings.Index(after, downMarker); downIdx != -1 {
		after = after[:downIdx]
	}
	return strings.TrimSpace(after)
}
