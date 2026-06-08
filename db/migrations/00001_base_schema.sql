-- +goose Up
-- Base workspace schema (migrations 00001–00014 from workflow-backend).
-- Equivalent to the state BEFORE the v003 owner additions (00015).

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
    source_path    text        NOT NULL DEFAULT '',
    source_hash    text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, feature_name),
    UNIQUE (workspace_id, feature_id)
);

CREATE TABLE IF NOT EXISTS workspace_tasks (
    id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id   uuid        NOT NULL REFERENCES workspaces(id),
    feature_id     uuid        NOT NULL REFERENCES workspace_features(id),
    feature_name   text        NOT NULL,
    task_id        uuid        NOT NULL DEFAULT gen_random_uuid(),
    task_name      text        NOT NULL,
    title          text        NOT NULL,
    repo           text,
    status         text,
    depends_on     jsonb       NOT NULL DEFAULT '[]'::jsonb,
    blocked_reason text,
    branch         text,
    execution      jsonb,
    pr             jsonb,
    workspace_pr   jsonb,
    source_path    text        NOT NULL DEFAULT '',
    source_hash    text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
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

-- +goose Down
DROP TABLE IF EXISTS workspace_activity_events;
DROP TABLE IF EXISTS workspace_tasks;
DROP TABLE IF EXISTS workspace_features;
DROP TABLE IF EXISTS workspaces;
