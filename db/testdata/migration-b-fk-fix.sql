-- Migration artifact: FK target correction (workflow-orchestrator T22).
--
-- This file is the migration source artifact for T23 (workflow-backend migration task).
-- T23 must copy this SQL into the next-numbered goose file in workflow-backend/migrations/
-- (after migration-a-owner.sql) and then delete this file from workflow-orchestrator once
-- the workflow-backend migration has been applied and verified in production.
--
-- Goose format: add +goose markers when writing the workflow-backend migration file.

-- +goose Up
-- Fix: workspace_tasks.feature_id must reference workspace_features.feature_id
-- (the public business-key UUID), not workspace_features.id (the surrogate PK).
-- A standalone UNIQUE constraint on feature_id is required so PostgreSQL can use
-- it as a single-column FK target.

ALTER TABLE workspace_tasks
    DROP CONSTRAINT IF EXISTS workspace_tasks_feature_id_fkey;

ALTER TABLE workspace_features
    DROP CONSTRAINT IF EXISTS workspace_features_feature_id_key;
ALTER TABLE workspace_features
    ADD CONSTRAINT workspace_features_feature_id_key UNIQUE (feature_id);

ALTER TABLE workspace_tasks
    ADD CONSTRAINT workspace_tasks_feature_id_fkey
    FOREIGN KEY (feature_id) REFERENCES workspace_features(feature_id);

-- +goose Down
ALTER TABLE workspace_tasks
    DROP CONSTRAINT IF EXISTS workspace_tasks_feature_id_fkey;

ALTER TABLE workspace_features
    DROP CONSTRAINT IF EXISTS workspace_features_feature_id_key;

ALTER TABLE workspace_tasks
    ADD CONSTRAINT workspace_tasks_feature_id_fkey
    FOREIGN KEY (feature_id) REFERENCES workspace_features(id);
