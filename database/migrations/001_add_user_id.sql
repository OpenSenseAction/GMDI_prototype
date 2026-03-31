-- Migration 001: Add user_id columns to all tables
--
-- Part of PR feat/db-add-user-id (multi-user RLS support, Phase 1).
-- Backward-compatible: existing parser continues to work unchanged.
--   - DEFAULT 'user1' on each user_id column means un-modified INSERT
--     statements (no user_id supplied) keep writing to the single user.
--   - UNIQUE (cml_id, sublink_id) on cml_metadata keeps the parser's
--     ON CONFLICT (cml_id, sublink_id) clause valid until PR3 updates it.
--   - update_cml_stats gains a second parameter (DEFAULT 'user1') so the
--     existing single-argument call site still compiles and runs correctly.
--
-- Apply this to an existing deployment with:
--   docker compose exec database psql -U myuser -d mydatabase \
--     -f /path/to/001_add_user_id.sql
--
-- Rollback: restore from the backup taken before running this file.

-- ---------------------------------------------------------------------------
-- Step 1: Add nullable user_id column to each table, back-fill existing rows
--         to 'user1', then tighten to NOT NULL with a DEFAULT for new rows.
-- ---------------------------------------------------------------------------

ALTER TABLE cml_data    ADD COLUMN IF NOT EXISTS user_id TEXT;
ALTER TABLE cml_metadata ADD COLUMN IF NOT EXISTS user_id TEXT;
ALTER TABLE cml_stats    ADD COLUMN IF NOT EXISTS user_id TEXT;

UPDATE cml_data    SET user_id = 'user1' WHERE user_id IS NULL;
UPDATE cml_metadata SET user_id = 'user1' WHERE user_id IS NULL;
UPDATE cml_stats    SET user_id = 'user1' WHERE user_id IS NULL;

ALTER TABLE cml_data    ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE cml_data    ALTER COLUMN user_id SET DEFAULT 'user1';
ALTER TABLE cml_metadata ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE cml_metadata ALTER COLUMN user_id SET DEFAULT 'user1';
ALTER TABLE cml_stats    ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE cml_stats    ALTER COLUMN user_id SET DEFAULT 'user1';

-- ---------------------------------------------------------------------------
-- Step 2: Update primary keys on cml_metadata and cml_stats to be
--         (…, user_id).  A UNIQUE (cml_id, sublink_id) index is kept on
--         cml_metadata so the parser's existing ON CONFLICT clause stays
--         valid; it will be dropped in PR3 once the parser is updated.
-- ---------------------------------------------------------------------------

ALTER TABLE cml_metadata DROP CONSTRAINT IF EXISTS cml_metadata_pkey;
ALTER TABLE cml_metadata ADD PRIMARY KEY (cml_id, sublink_id, user_id);
-- Keep for backward compat with parser's ON CONFLICT (cml_id, sublink_id).
-- TODO: drop this constraint in PR3 (feat/parser-user-id).
ALTER TABLE cml_metadata
    ADD CONSTRAINT cml_metadata_sublink_unique UNIQUE (cml_id, sublink_id);

ALTER TABLE cml_stats DROP CONSTRAINT IF EXISTS cml_stats_pkey;
ALTER TABLE cml_stats ADD PRIMARY KEY (cml_id, user_id);

-- ---------------------------------------------------------------------------
-- Step 3: Add indexes for per-user query performance.
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_cml_data_user_id     ON cml_data     (user_id);
CREATE INDEX IF NOT EXISTS idx_cml_metadata_user_id ON cml_metadata  (user_id);

-- ---------------------------------------------------------------------------
-- Step 4: Replace update_cml_stats with a version that accepts an optional
--         target_user_id (DEFAULT 'user1') for backward compatibility.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_cml_stats(
    target_cml_id  TEXT,
    target_user_id TEXT DEFAULT 'user1'
) RETURNS VOID AS $$
BEGIN
    INSERT INTO cml_stats (
        cml_id,
        user_id,
        total_records,
        valid_records,
        null_records,
        completeness_percent,
        min_rsl,
        max_rsl,
        mean_rsl,
        stddev_rsl,
        last_rsl,
        last_update
    )
    SELECT
        cd.cml_id::text,
        target_user_id,
        COUNT(*)                                                              AS total_records,
        COUNT(CASE WHEN cd.rsl IS NOT NULL THEN 1 END)                        AS valid_records,
        COUNT(CASE WHEN cd.rsl IS NULL     THEN 1 END)                        AS null_records,
        ROUND(
            100.0 * COUNT(CASE WHEN cd.rsl IS NOT NULL THEN 1 END) / COUNT(*),
            2
        )                                                                     AS completeness_percent,
        MIN(cd.rsl)                                                           AS min_rsl,
        MAX(cd.rsl)                                                           AS max_rsl,
        ROUND(AVG(cd.rsl)::numeric,    2)                                     AS mean_rsl,
        ROUND(STDDEV(cd.rsl)::numeric, 2)                                     AS stddev_rsl,
        (
            SELECT rsl FROM cml_data
            WHERE  cml_id  = cd.cml_id
              AND  user_id = target_user_id
            ORDER  BY time DESC LIMIT 1
        )                                                                     AS last_rsl,
        NOW()
    FROM cml_data cd
    WHERE cd.cml_id  = target_cml_id
      AND cd.user_id = target_user_id
    GROUP BY cd.cml_id
    ON CONFLICT (cml_id, user_id) DO UPDATE SET
        total_records        = EXCLUDED.total_records,
        valid_records        = EXCLUDED.valid_records,
        null_records         = EXCLUDED.null_records,
        completeness_percent = EXCLUDED.completeness_percent,
        min_rsl              = EXCLUDED.min_rsl,
        max_rsl              = EXCLUDED.max_rsl,
        mean_rsl             = EXCLUDED.mean_rsl,
        stddev_rsl           = EXCLUDED.stddev_rsl,
        last_rsl             = EXCLUDED.last_rsl,
        last_update          = EXCLUDED.last_update;
END;
$$ LANGUAGE plpgsql;
