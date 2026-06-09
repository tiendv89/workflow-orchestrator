-- Migration artifact: owner discriminator columns (workflow-orchestrator T22).
--
-- This file is the migration source artifact for T23 (workflow-backend migration task).
-- T23 must copy this SQL into the next-numbered goose file in workflow-backend/migrations/
-- and then delete this file from workflow-orchestrator once the workflow-backend migration
-- has been applied and verified in production.
--
-- Goose format: add +goose markers when writing the workflow-backend migration file.

-- +goose Up
ALTER TABLE workspace_features
    ADD COLUMN IF NOT EXISTS owner text,
    ALTER COLUMN source_path DROP NOT NULL,
    ALTER COLUMN source_path DROP DEFAULT;

ALTER TABLE workspace_tasks
    ADD COLUMN IF NOT EXISTS owner text,
    ALTER COLUMN source_path DROP NOT NULL,
    ALTER COLUMN source_path DROP DEFAULT;

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
