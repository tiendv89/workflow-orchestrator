-- name: InsertHandoffPR :one
-- Creates a new handoff PR row. ON CONFLICT DO NOTHING is idempotent per UNIQUE(handoff_id, repo).
INSERT INTO handoff_prs (handoff_id, repo, pr_url, status)
VALUES ($1, $2, $3, $4)
ON CONFLICT (handoff_id, repo) DO NOTHING
RETURNING id, handoff_id, repo, pr_url, status, conflict_state, rebase_attempts,
          dispatch_handle, dispatch_nonce, dispatched_at, reenqueue_attempts, created_at;

-- name: ListHandoffPRsByHandoff :many
SELECT id, handoff_id, repo, pr_url, status, conflict_state, rebase_attempts,
       dispatch_handle, dispatch_nonce, dispatched_at, reenqueue_attempts, created_at
FROM handoff_prs
WHERE handoff_id = $1
ORDER BY created_at ASC;

-- name: UpdateHandoffPRStatus :exec
UPDATE handoff_prs
SET status = $2
WHERE id = $1;

-- name: UpdateHandoffPRConflictState :exec
UPDATE handoff_prs
SET conflict_state = $2
WHERE id = $1;

-- name: ListResolvingHandoffPRs :many
-- Returns handoff PRs currently in rebase dispatch (for the reconciler).
SELECT id, handoff_id, repo, pr_url, status, conflict_state, rebase_attempts,
       dispatch_handle, dispatch_nonce, dispatched_at, reenqueue_attempts, created_at
FROM handoff_prs
WHERE conflict_state = 'resolving';
