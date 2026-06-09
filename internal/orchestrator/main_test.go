package orchestrator_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/tiendv89/workflow-orchestrator/internal/database"
)

// TestMain applies the schema snapshot before any test in this package runs,
// so that unit tests that connect to DATABASE_URL find the schema ready.
//
// If DATABASE_URL is unset, the snapshot is skipped — individual tests that
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

	raw, err := os.ReadFile("../../db/testdata/schema.sql")
	if err != nil {
		return fmt.Errorf("read schema snapshot: %w", err)
	}
	if _, err := pool.Exec(ctx, string(raw)); err != nil {
		return fmt.Errorf("apply schema snapshot: %w", err)
	}
	return nil
}
