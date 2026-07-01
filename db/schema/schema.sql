-- schema.sql — workspace v004 schema snapshot
-- Derived from workflow-backend migrations 00001–00017 (v004 dispatch+handoff additions).
-- This file is the sqlc schema source for workflow-orchestrator.
-- Source of truth: workflow-backend/migrations; see database/workspace/v004/schema.dbml.

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
    UNIQUE (workspace_id, feature_id)
);

CREATE TABLE IF NOT EXISTS workspace_tasks (
    id                      uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id            uuid        NOT NULL REFERENCES workspaces(id),
    feature_id              uuid        NOT NULL REFERENCES workspace_features(id),
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
    dispatch_handle         text,
    dispatch_nonce          text,
    dispatched_at           timestamptz,
    reenqueue_attempts      int         NOT NULL DEFAULT 0,
    dispatch_kind           text,
    review_incomplete_count int         NOT NULL DEFAULT 0,
    max_turns_retry_count   int         NOT NULL DEFAULT 0,
    rebase_attempts         int         NOT NULL DEFAULT 0,
    conflict_state          text        NOT NULL DEFAULT 'none',
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

CREATE TABLE IF NOT EXISTS handoffs (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id uuid        NOT NULL REFERENCES workspaces(id),
    feature_id   uuid        NOT NULL REFERENCES workspace_features(id),
    mgmt_pr_url  text,
    status       text        NOT NULL DEFAULT 'open',
    created_at   timestamptz NOT NULL DEFAULT now(),
    finalized_at timestamptz,
    UNIQUE (feature_id)
);

CREATE TABLE IF NOT EXISTS handoff_prs (
    id                 uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    handoff_id         uuid        NOT NULL REFERENCES handoffs(id) ON DELETE CASCADE,
    repo               text        NOT NULL,
    pr_url             text,
    status             text        NOT NULL DEFAULT 'open',
    conflict_state     text        NOT NULL DEFAULT 'none',
    rebase_attempts    int         NOT NULL DEFAULT 0,
    dispatch_handle    text,
    dispatch_nonce     text,
    dispatched_at      timestamptz,
    reenqueue_attempts int         NOT NULL DEFAULT 0,
    created_at         timestamptz NOT NULL DEFAULT now(),
    UNIQUE (handoff_id, repo)
);
