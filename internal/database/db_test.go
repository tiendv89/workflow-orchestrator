package database_test

import (
	"context"
	"os"
	"testing"

	"github.com/tiendv89/workflow-orchestrator/internal/database"
)

// TestOpen_ConnectAndPing verifies that Open connects to the real Postgres
// instance and that a SELECT 1 round-trip succeeds.
// Requires DATABASE_URL to be set; skips gracefully when it is not.
func TestOpen_ConnectAndPing(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set — skipping integration test")
	}

	ctx := context.Background()
	pool, err := database.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("database.Open failed: %v", err)
	}
	defer database.Close(pool)

	var result int
	row := pool.QueryRow(ctx, "SELECT 1")
	if err := row.Scan(&result); err != nil {
		t.Fatalf("SELECT 1 failed: %v", err)
	}
	if result != 1 {
		t.Errorf("SELECT 1 = %d, want 1", result)
	}
}

// TestOpen_InvalidURL verifies that a bad DSN returns an error rather than panicking.
func TestOpen_InvalidURL(t *testing.T) {
	ctx := context.Background()
	pool, err := database.Open(ctx, "postgres://invalid-host-that-does-not-exist/db")
	if err == nil {
		pool.Close()
		t.Fatal("expected error for invalid DSN, got nil")
	}
}

// TestClose_NilPool verifies that Close does not panic on a nil pool.
func TestClose_NilPool(t *testing.T) {
	database.Close(nil)
}
