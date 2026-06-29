package orchestrator_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tiendv89/workflow-orchestrator/internal/database"
)

// openPool returns a connected pool or skips the test if DATABASE_URL is unset.
func openPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set — skipping integration test")
	}
	pool, err := database.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("database.Open: %v", err)
	}
	t.Cleanup(func() { database.Close(pool) })
	return pool
}
