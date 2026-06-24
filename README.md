# workflow-orchestrator

The Go orchestrator is **execution-only**. It polls the database for eligible
tasks, claims them, dispatches them to the agent broker, reaps results, and
advances task status — nothing more.

## Task and feature creation

Task and feature creation is handled exclusively by the **workflow-backend API**
(§4.8 of the technical design). Use the `POST /workspaces/:id/features/:fid/tasks`
endpoint (or the `create-tasks` workflow skill) to materialize tasks into the
database before the orchestrator picks them up.

The orchestrator no longer contains a creation path. `internal/orchestrator/create.go`
and `cmd/seed` have been removed from the production build. Fixture helpers for
seeding test data in integration tests live behind the `integration` build tag
(`//go:build integration`) and are not included in production binaries.

## Running

```
WORKSPACE_ID=<uuid> DATABASE_URL=<dsn> REDIS_URL=<url> BROKER_URL=<url> GITHUB_TOKEN=<token> \
  go run ./cmd/orchestrator
```

## Tests

Unit tests (no database required):

```
go test ./...
```

Integration + E2E tests (requires `DATABASE_URL` or Docker for testcontainers):

```
go test ./... -tags integration
go test ./test/e2e/... -tags integration
```
