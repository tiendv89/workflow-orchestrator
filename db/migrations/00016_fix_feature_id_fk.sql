-- +goose Up
-- Fix: workspace_tasks.feature_id must reference workspace_features.feature_id
-- (the public business-key UUID), not workspace_features.id (the surrogate PK).
-- All application code, helper queries, and E2E test joins use the feature_id
-- business key for cross-table lookups; the original FK target was incorrect.
--
-- A standalone UNIQUE constraint on feature_id is required so PostgreSQL can use
-- it as a single-column FK target (the existing UNIQUE(workspace_id, feature_id)
-- composite index is not sufficient for a single-column FK reference).

-- Drop FK first; it depends on the UNIQUE constraint below.
ALTER TABLE workspace_tasks
    DROP CONSTRAINT IF EXISTS workspace_tasks_feature_id_fkey;

-- Recreate the UNIQUE constraint on the business key (safe now that FK is gone).
ALTER TABLE workspace_features
    DROP CONSTRAINT IF EXISTS workspace_features_feature_id_key;
ALTER TABLE workspace_features
    ADD CONSTRAINT workspace_features_feature_id_key UNIQUE (feature_id);

-- Recreate the FK pointing to the business key.
ALTER TABLE workspace_tasks
    ADD CONSTRAINT workspace_tasks_feature_id_fkey
    FOREIGN KEY (feature_id) REFERENCES workspace_features(feature_id);

-- +goose Down
ALTER TABLE workspace_tasks
    DROP CONSTRAINT workspace_tasks_feature_id_fkey;

ALTER TABLE workspace_features
    DROP CONSTRAINT workspace_features_feature_id_key;

ALTER TABLE workspace_tasks
    ADD CONSTRAINT workspace_tasks_feature_id_fkey
    FOREIGN KEY (feature_id) REFERENCES workspace_features(id);
