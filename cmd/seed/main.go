// seed inserts a go-owned feature and its tasks into the DB from a JSON fixture.
//
// Usage:
//
//	seed --fixture <path> --db <database-url>
//
// The DATABASE_URL environment variable is used as fallback when --db is omitted.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/tiendv89/workflow-orchestrator/internal/database"
	"github.com/tiendv89/workflow-orchestrator/internal/orchestrator"
)

func main() {
	fixturePath := flag.String("fixture", "", "path to JSON fixture file (required)")
	dbURL := flag.String("db", "", "database URL (overrides DATABASE_URL env var)")
	flag.Parse()

	if *fixturePath == "" {
		log.Fatal("--fixture is required")
	}

	dsn := *dbURL
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		log.Fatal("database URL required: set --db flag or DATABASE_URL env var")
	}

	data, err := os.ReadFile(*fixturePath)
	if err != nil {
		log.Fatalf("read fixture %q: %v", *fixturePath, err)
	}

	var spec orchestrator.GoFeatureSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		log.Fatalf("parse fixture: %v", err)
	}

	ctx := context.Background()
	pool, err := database.Open(ctx, dsn)
	if err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	defer database.Close(pool)

	if err := orchestrator.MaterializeFeature(ctx, pool, spec); err != nil {
		log.Fatalf("MaterializeFeature: %v", err)
	}

	fmt.Printf("OK: feature %q materialized in workspace %s\n", spec.Slug, spec.WorkspaceID)
}
