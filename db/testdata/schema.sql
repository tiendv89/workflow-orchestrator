-- Test-only schema snapshot for workflow-orchestrator.
--
-- This file is the authoritative source for test database setup. It represents
-- the combined final state after:
--   • workflow-backend/migrations/00001–00014 (base workspace schema)
--   • workflow-backend/migrations/00015_*_owner (owner discriminator + relaxed source_path)
--   • workflow-backend/migrations/00016_*_fix_feature_id_fk (FK target correction)
--
-- The 00015 and 00016 equivalents are committed as separate goose files in
-- db/testdata/migration-a-owner.sql and db/testdata/migration-b-fk-fix.sql,
-- which serve as the migration artifact for T23 (workflow-backend migration task).
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
    id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id   uuid        NOT NULL REFERENCES workspaces(id),
    feature_id     uuid        NOT NULL REFERENCES workspace_features(feature_id),
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
    source_path    text,
    source_hash    text,
    owner          text,
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

CREATE INDEX IF NOT EXISTS workspace_features_workspace_owner
    ON workspace_features (workspace_id, owner);

CREATE INDEX IF NOT EXISTS workspace_tasks_workspace_owner_status
    ON workspace_tasks (workspace_id, owner, status);
