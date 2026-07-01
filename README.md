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

## State machines

This section is the authoritative reference for agents and developers implementing
or extending the orchestrator. It maps directly to the approved technical design
(`go-orchestrator-autonomy`).

### Task status FSM

| From | To | Trigger | Guard / actor |
|---|---|---|---|
| `todo` | `ready` | all `depends_on` done | auto-ready rule |
| `ready` | `in_progress` | impl claim | guarded `ready→in_progress` |
| `change_requested` | `in_progress` | fix claim | guarded |
| `in_progress` | `in_review` | impl/fix complete | reap |
| `in_progress` | `ready` | max-turns retry (`< EXECUTOR_MAX_RETRIES`) | reap (`max_turns`) |
| `in_progress` | `blocked` | reconciler-max / DLQ `failed` / agent block / max-turns cap | reconciler/reap |
| `in_review` | `reviewing` | reviewer dispatch | guarded `in_review→reviewing` |
| `review_incomplete` | `reviewing` | reviewer re-dispatch | guarded |
| `reviewing` | `review_passed` | APPROVE verdict | reap |
| `reviewing` | `change_requested` | REQUEST_CHANGES or reviewer 409 (Path A conflict) | reap |
| `reviewing` | `review_incomplete` | no valid verdict (`< MAX_REVIEW_INCOMPLETES`) | reap |
| `review_incomplete` | `blocked` | no valid verdict (`>= MAX_REVIEW_INCOMPLETES`) | reap |
| `in_review` / `reviewing` / `review_passed` | `done` | impl PR **observed merged** — ground truth; covers reviewer-auto-merged-then-crashed-before-verdict-reaped race | `handleMergedPrs`, guarded `WHERE status IN ('in_review','reviewing','review_passed')` |
| `blocked` | `ready` / `in_review` | human unblock (cause-aware, derived from `blocked_from_status`) | human via unblock API |
| any **non-terminal** | `cancelled` | human | human, guarded `WHERE status NOT IN ('done','cancelled')` |

**Terminal-state protection:** `done` and `cancelled` are terminal. `SetBlocked` and every
`*`-wildcard guard must use `WHERE status NOT IN ('done','cancelled')`. Reap drops completions
whose task row is already terminal. This composes with merged-is-ground-truth: a racing
failed/blocked completion no-ops against a `done` row.

### conflict_state FSM (tasks and handoff_prs)

```
none → conflicted → resolving → resolved
                ↑       ↓
                └── conflicted   (retriable failure < MAX_REBASE_ATTEMPTS)
```

- `none → conflicted`: poll detects `mergeable=CONFLICTING`
- `conflicted → resolving`: rebase executor dispatched (guarded — the `resolving` status is the
  multi-instance duplicate-dispatch guard; equivalent to TS's in-memory `rebaseInFlight`)
- `resolving → resolved`: rebase succeeds
- `resolving → conflicted`: rebase fails, retries remain (`< MAX_REBASE_ATTEMPTS`)
- Rebase cap exceeded:
  - **Path A** (auto-merge, `in_review`): `→ blocked (rebase_failed)`
  - **Path B** (human-merge, `review_passed`) / handoff PR: stay + `conflicted` + Slack (do NOT block)

**INVARIANT:** `resolving` ⟺ a rebase executor is in-flight. Every completion (success or failure)
exits `resolving`. No row may be left in `resolving` without a live executor.

### feature_status FSM (owner split)

```
in_design → in_tdd → ready_for_implementation   (adapter, from status.yaml git sync)
    → in_implementation                          (orchestrator, first task dispatch)
    → in_handoff                                 (orchestrator, handoff trigger)
    → done                                       (orchestrator, finalize — all handoff PRs merged)
any → cancelled
```

**Owner rule:** the Go orchestrator exclusively owns `in_implementation`, `in_handoff`, and
`done`-at-finalize writes. The `workspace-github-adapter` must **not** clobber those values:
it gates its `feature_status` writes on the **current DB value** (if the DB already holds
`in_implementation`/`in_handoff`, the adapter skips the write); `cancelled`/`done`
(finalize) still win.

### Per-task/PR counters and reset rules

| Column | Scope | Reset rule |
|---|---|---|
| `dispatch_handle` / `dispatch_nonce` / `dispatched_at` | per-dispatch | set on dispatch-in; cleared on dispatch-out |
| `reenqueue_attempts` | per-dispatch | `0` on dispatch-in; cleared on dispatch-out (**not** on enqueue-success) |
| `max_turns_retry_count` | per-work-episode | `0` on success exit `→in_review`; also `0` on unblock |
| `review_incomplete_count` | per-review | reset when review concludes; also `0` on unblock |
| `rebase_attempts` | per-conflict-episode | `0` on `conflict_state=resolved`; also `0` on unblock |
| `blocked_from_status` | — | set on every `→blocked`; read on unblock to derive resume state |

**Unblock MUST reset the counter that caused the block**, otherwise the task re-blocks
immediately with zero budget:
- `review_incomplete_count=0` when `blocked_reason=review_incomplete_exceeded`
- `rebase_attempts=0` + clear `conflict_state` when `blocked_reason=rebase_failed`
- `max_turns_retry_count=0` when `blocked_reason=max_turns_exceeded`

Crash/spawn/agent blocks need no explicit counter reset (`reenqueue_attempts` is
per-dispatch and is cleared on the next dispatch-in).

### In-flight (soft-claim) predicate

```
inflight =
  count(workspace_tasks WHERE status IN ('in_progress','reviewing') OR conflict_state='resolving')
  + count(handoff_prs WHERE conflict_state='resolving')

headroom = MAX_INFLIGHT − inflight
```

Applied before **every** dispatch kind against one shared budget. Soft by design (bounded
multi-instance overshoot; the dispatcher `DISPATCHER_MAX_CONCURRENT` remains the hard cap).
Derived each cycle from the DB — no maintained `+1/-1` counter to avoid drift/leak under
crash or reconcile.

### Per-poll cycle order

```
1. feature-branch / handoff triggers
2. handoff-PR conflict-rebase + finalize   (HIGH priority — close features fast)
3. task dispatch (claim / fix / reviewer / task-rebase)   with leftover headroom
4. reap
5. dispatch reconciler
```

Steps 2 and 3 share the same soft-claim budget; step 2 runs first so handoff-PR rebases
consume headroom before task dispatch.

### DB design cross-reference

New columns on `workspace_tasks` (additive, nullable-or-defaulted):
`dispatch_handle`, `dispatch_nonce`, `dispatched_at`, `reenqueue_attempts`,
`review_incomplete_count`, `max_turns_retry_count`, `rebase_attempts`,
`conflict_state` (default `'none'`), `blocked_from_status`, `dispatch_kind`.

New tables: `handoffs` (one row per feature handoff; `UNIQUE(feature_id)` is the
multi-instance trigger guard) and `handoff_prs` (one row per impl-repo handoff PR;
`UNIQUE(handoff_id, repo)`). Partial indexes on both for the per-cycle in-flight count.

Full schema design: `go-orchestrator-autonomy` technical design §"DB design".

### Unblock API cross-reference

`POST /api/workspaces/:workspaceId/features/:featureId/tasks/:taskId/unblock`

The resume state is **derived server-side** from `blocked_from_status` — no caller-chosen
target. Mapping: `in_progress → ready`; `reviewing`/`in_review` → `in_review`.

Surface: backend endpoint → bff proxy → `workflow-mcp` `unblock_task` tool →
`unblock-task` agent skill.

Full API contract: `go-orchestrator-autonomy` technical design §"API design".

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
