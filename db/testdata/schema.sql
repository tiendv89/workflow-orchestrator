-- Test-only schema snapshot for workflow-orchestrator.
--
-- This file is the authoritative source for test database setup. It represents
-- the combined final state after:
--   • workflow-backend/migrations/00001–00014 (base workspace schema)
--   • workflow-backend/migrations/00015_*_owner (owner discriminator + relaxed source_path)
--   • workflow-backend/migrations/00016_*_fix_feature_id_fk (FK target correction)
--   • workflow-backend/migrations/00017_*_task_dispatch (T1: dispatch columns + handoffs/handoff_prs)
--
-- DO NOT add goose markers. This file is executed as a single SQL batch by
-- TestMain in internal/orchestrator/ and test/e2e/.
--
-- Keep in sync with workflow-backend's canonical migration chain.

CREATE TABLE IF NOT EXISTS workspaces (
    id                 uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id    uuid        NOT NULL,
    slug               text        UNIQUE NOT NULL,
    name               text        NOT NULL,
    management_repo_id text        NOT NULL,
    branch_pattern     text,
    slack_channel_id   text,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workspace_features (
    id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id   uuid        NOT NULL REFERENCES workspaces(id),
    feature_id     uuid        NOT NULL DEFAULT gen_random_uuid(),
    feature_name   text        NOT NULL,
    title          text        NOT NULL,
    feature_status text,
    current_stage  text,
    next_action    text,
    stages         jsonb,
    source_path    text,
    source_hash    text,
    owner          text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, feature_name),
    UNIQUE (workspace_id, feature_id),
    UNIQUE (feature_id)
);

-- workspace_tasks.feature_id references workspace_features(feature_id), the
-- business-key UUID, not workspace_features.id (the surrogate PK). This matches
-- the corrected FK introduced in migration 00016.
CREATE TABLE IF NOT EXISTS workspace_tasks (
    id                      uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id            uuid        NOT NULL REFERENCES workspaces(id),
    feature_id              uuid        NOT NULL REFERENCES workspace_features(feature_id),
    feature_name            text        NOT NULL,
    task_id                 uuid        NOT NULL DEFAULT gen_random_uuid(),
    task_name               text        NOT NULL,
    title                   text        NOT NULL,
    repo                    text,
    status                  text,
    depends_on              jsonb       NOT NULL DEFAULT '[]'::jsonb,
    blocked_reason          text,
    blocked_details         text,
    branch                  text,
    execution               jsonb,
    pr                      jsonb,
    workspace_pr            jsonb,
    source_path             text,
    source_hash             text,
    owner                   text,
    -- dispatch metadata (set on claim/dispatch-in; cleared on dispatch-out)
    dispatch_handle         text,
    dispatch_nonce          text,
    dispatched_at           timestamptz,
    reenqueue_attempts      int         NOT NULL DEFAULT 0,
    dispatch_kind           text,
    -- per-review / per-work-episode counters
    review_incomplete_count int         NOT NULL DEFAULT 0,
    max_turns_retry_count   int         NOT NULL DEFAULT 0,
    -- conflict resolution
    rebase_attempts         int         NOT NULL DEFAULT 0,
    conflict_state          text        NOT NULL DEFAULT 'none',
    -- unblock resume
    blocked_from_status     text,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, feature_id, task_name),
    UNIQUE (workspace_id, task_id)
);

CREATE TABLE IF NOT EXISTS workspace_activity_events (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid        NOT NULL REFERENCES workspaces(id),
    scope_type   text        NOT NULL,
    feature_id   uuid,
    feature_name text,
    task_id      uuid,
    task_name    text,
    action       text,
    actor        text,
    occurred_at  text,
    note         text,
    sequence     integer     NOT NULL,
    raw_event    jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workspace_repos (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid        NOT NULL REFERENCES workspaces(id),
    repo_id      text        NOT NULL,
    base_branch  text,
    repo_url     text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, repo_id)
);

-- one row per feature handoff; UNIQUE(feature_id) is the multi-instance trigger guard
CREATE TABLE IF NOT EXISTS handoffs (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid        NOT NULL REFERENCES workspaces(id),
    feature_id   uuid        NOT NULL REFERENCES workspace_features(feature_id),
    mgmt_pr_url  text,
    status       text        NOT NULL DEFAULT 'open',
    created_at   timestamptz NOT NULL DEFAULT now(),
    finalized_at timestamptz,
    UNIQUE (feature_id)
);

-- one row per impl-repo PR per handoff; drives the handoff-PR rebase loop
CREATE TABLE IF NOT EXISTS handoff_prs (
    id                 uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    handoff_id         uuid        NOT NULL REFERENCES handoffs(id) ON DELETE CASCADE,
    repo               text        NOT NULL,
    pr_url             text,
    status             text        NOT NULL DEFAULT 'open',
    conflict_state     text        NOT NULL DEFAULT 'none',
    rebase_attempts    int         NOT NULL DEFAULT 0,
    -- dispatch metadata mirrors workspace_tasks columns (for rebase-dispatch reconciler)
    dispatch_handle    text,
    dispatch_nonce     text,
    dispatched_at      timestamptz,
    reenqueue_attempts int         NOT NULL DEFAULT 0,
    created_at         timestamptz NOT NULL DEFAULT now(),
    UNIQUE (handoff_id, repo)
);

-- existing indexes
CREATE INDEX IF NOT EXISTS workspace_features_workspace_owner
    ON workspace_features (workspace_id, owner);

CREATE INDEX IF NOT EXISTS workspace_tasks_workspace_owner_status
    ON workspace_tasks (workspace_id, owner, status);

-- partial index for the per-cycle in-flight count (tasks half) and reconciler scan
CREATE INDEX IF NOT EXISTS workspace_tasks_inflight
    ON workspace_tasks (workspace_id)
    WHERE owner = 'go'
      AND (status IN ('in_progress', 'reviewing') OR conflict_state = 'resolving');

-- partial index for the per-cycle in-flight count (handoff_prs half) and reconciler scan
CREATE INDEX IF NOT EXISTS handoff_prs_resolving
    ON handoff_prs (handoff_id)
    WHERE conflict_state = 'resolving';

-- FK-join index for the finalize check (all PRs of a handoff merged?)
-- UNIQUE(handoff_id, repo) already has handoff_id as prefix; this explicit index
-- is redundant but added for clarity per design spec.
CREATE INDEX IF NOT EXISTS handoff_prs_handoff_id
    ON handoff_prs (handoff_id);
