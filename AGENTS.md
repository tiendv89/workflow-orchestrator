# AGENTS.md — workflow-orchestrator

This file is the agent-facing companion to `README.md`. It summarises the
state-machine contracts that agents must respect when interacting with the
orchestrator.

## Task status transitions you can rely on

Agents receive tasks via the broker and are responsible for:

1. **Claiming** a `ready` task (`ready → in_progress`, guarded UPDATE).
2. **Completing** implementation and posting results via `result.json` (orchestrator
   reaps → `in_progress → in_review`).
3. **Reviewing** a task when dispatched as a reviewer (`in_review → reviewing`,
   guarded). Verdict routes: APPROVE → `review_passed`; REQUEST_CHANGES →
   `change_requested`; no valid verdict → `review_incomplete`.
4. **Fixing** after a review (`change_requested → in_progress`, guarded).
5. **Rebasing** a conflicted task PR or handoff PR when dispatched as a rebase
   agent (`conflict_state: resolving` while in-flight).

The orchestrator handles all other transitions (auto-ready, max-turns retry,
reconciler, PR-merge poll, handoff finalize). **Do not directly mutate task
status** outside of the claim protocol.

## Terminal-state rule

`done` and `cancelled` are terminal. Never attempt to transition a task out of
those states. The guarded UPDATE will no-op on terminal rows, so a race against a
late verdict or block is safe — but rely on the guard, not on reading first.

## Blocking a task

If you must block a task, set `result.json` with:

```json
{
  "terminal_status": "blocked",
  "blocked_reason": "<short-slug>",
  "blocked_suggestion": "<concrete next step>"
}
```

Before marking a task blocked, **commit and push all in-progress work** to the
task branch (even if partial/broken). Use a `wip(<task-id>): <state>` commit
message. An agent that blocks without committing forces the next agent to restart
from scratch.

## Counters you must not reset directly

The orchestrator manages `review_incomplete_count`, `rebase_attempts`,
`max_turns_retry_count`, and `reenqueue_attempts`. Do not write to these columns.
Their reset rules are tied to specific lifecycle events — see `README.md §
Per-task/PR counters and reset rules`.

## Conflict state during rebase

When you are dispatched as a rebase agent, the task's `conflict_state` is
`resolving`. Every exit path (success or failure) must leave `resolving` — the
orchestrator transitions it to `resolved` or back to `conflicted` based on your
`result.json`. An executor that exits without a result leaves `resolving` stuck;
the reconciler handles this via `EXECUTION_DEADLINE_MS`.

## Soft-claim headroom

The orchestrator enforces a soft concurrency limit (`MAX_INFLIGHT`). Dispatch
is withheld when `inflight ≥ MAX_INFLIGHT`. The in-flight count is derived:

```
inflight =
  count(tasks WHERE status IN ('in_progress','reviewing') OR conflict_state='resolving')
  + count(handoff_prs WHERE conflict_state='resolving')
```

This is a soft limit — the dispatcher `DISPATCHER_MAX_CONCURRENT` is the hard cap.

## Unblock resume targets

When a human unblocks a task via the API, the resume state is derived from
`blocked_from_status`:

| `blocked_from_status` | Resume state |
|---|---|
| `in_progress` | `ready` (re-claim) |
| `reviewing` | `in_review` (re-dispatch reviewer) |
| `in_review` | `in_review` (re-dispatch reviewer) |

**The counter that caused the block is reset on unblock** — the task will not
immediately re-block when picked up again. The unblock API also clears
`conflict_state` when `blocked_reason=rebase_failed`.

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `MAX_INFLIGHT` | — | Soft in-flight claim cap |
| `MAX_REBASE_ATTEMPTS` | `3` | Per-conflict-episode rebase cap |
| `EXECUTION_DEADLINE_MS` | `7200000` (2 h) | Reconciler dispatch deadline |
| `DISPATCH_RECONCILE_MAX_RETRIES` | `3` | Reconciler re-enqueue cap |
| `EXECUTOR_MAX_RETRIES` | `3` | Max-turns reset cap |
| `MAX_REVIEW_INCOMPLETES` | `2` | Reviewer no-valid-verdict retry cap |
| `DISPATCHER_MAX_CONCURRENT` | `5` | Hard dispatcher concurrency cap |
| `DISPATCHER_MAX_DELIVERIES` | `5` | DLQ delivery cap before synthetic `failed` |
