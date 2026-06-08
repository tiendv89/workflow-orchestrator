-- +goose Up
-- Migration 00015: v003 owner additions (workflow-backend/migrations/00015_*_owner).
-- Adds nullable owner discriminator to workspace_features and workspace_tasks.
-- NULL/absent = legacy git/YAML (TS orchestrator); 'go' = DB-native (Go orchestrator).
-- Also relaxes source_path to nullable (go-owned rows have no YAML origin).

ALTER TABLE workspace_features
    ADD COLUMN IF NOT EXISTS owner text,
    ALTER COLUMN source_path DROP NOT NULL,
    ALTER COLUMN source_path DROP DEFAULT;

ALTER TABLE workspace_tasks
    ADD COLUMN IF NOT EXISTS owner text,
    ALTER COLUMN source_path DROP NOT NULL,
    ALTER COLUMN source_path DROP DEFAULT;

-- Indexes for eligibility scan and sync adapter scoping.
CREATE INDEX IF NOT EXISTS workspace_features_workspace_owner
    ON workspace_features (workspace_id, owner);

CREATE INDEX IF NOT EXISTS workspace_tasks_workspace_owner_status
    ON workspace_tasks (workspace_id, owner, status);

-- +goose Down
DROP INDEX IF EXISTS workspace_tasks_workspace_owner_status;
DROP INDEX IF EXISTS workspace_features_workspace_owner;

ALTER TABLE workspace_tasks
    DROP COLUMN IF EXISTS owner,
    ALTER COLUMN source_path SET NOT NULL,
    ALTER COLUMN source_path SET DEFAULT '';

ALTER TABLE workspace_features
    DROP COLUMN IF EXISTS owner,
    ALTER COLUMN source_path SET NOT NULL,
    ALTER COLUMN source_path SET DEFAULT '';
