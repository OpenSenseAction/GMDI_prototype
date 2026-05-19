-- Migration 009: add windowed cml_stats columns and update_cml_stats_windowed function
--
-- Replaces the expensive full-history refresh (update_cml_stats, called for every CML
-- every 60 seconds) with a cheap windowed refresh that only touches the current
-- uncompressed TimescaleDB chunk (~6 hours of data), reducing stats refresh time from
-- 20-30 s to < 1 s and Postgres CPU from ~100% to < 5%.
--
-- New columns on cml_stats: 6-hour and 1-hour windowed completeness, record counts,
-- mean RSL, and stddev RSL.
-- New function: update_cml_stats_windowed(target_cml_id, target_user_id)
--
-- The parser must be updated alongside this migration:
--   - parser/db_writer.py: replace refresh_stats() call with refresh_windowed_stats()
--   - parser/db_writer.py: wire _update_stats_for_cmls() into write_rawdata() so
--     lifetime columns (total_records, min_rsl, max_rsl, ...) stay current.
--
-- Apply with:
--   docker compose exec -T database psql -U myuser -d mydatabase \
--     < database/migrations/009_add_windowed_stats.sql

ALTER TABLE cml_stats
    ADD COLUMN IF NOT EXISTS completeness_percent_6h REAL,
    ADD COLUMN IF NOT EXISTS total_records_6h        BIGINT,
    ADD COLUMN IF NOT EXISTS valid_records_6h        BIGINT,
    ADD COLUMN IF NOT EXISTS mean_rsl_6h             REAL,
    ADD COLUMN IF NOT EXISTS stddev_rsl_6h           REAL,
    ADD COLUMN IF NOT EXISTS completeness_percent_1h REAL,
    ADD COLUMN IF NOT EXISTS mean_rsl_1h             REAL,
    ADD COLUMN IF NOT EXISTS stddev_rsl_1h           REAL;

-- update_cml_stats_windowed(target_cml_id, target_user_id)
--
-- Computes 6-hour and 1-hour windowed statistics in a single table scan using
-- FILTER clauses.  TimescaleDB chunk exclusion prunes all chunks older than 6 hours
-- at the storage level, so the query only touches the current uncompressed chunk
-- regardless of total dataset size.
CREATE OR REPLACE FUNCTION update_cml_stats_windowed(
    target_cml_id  TEXT,
    target_user_id TEXT DEFAULT 'demo_openmrg'
) RETURNS VOID AS $$
DECLARE
    now_ts TIMESTAMPTZ := NOW();
BEGIN
    INSERT INTO cml_stats (
        cml_id, user_id,
        completeness_percent_6h, total_records_6h, valid_records_6h,
        mean_rsl_6h,  stddev_rsl_6h,
        completeness_percent_1h, mean_rsl_1h, stddev_rsl_1h,
        last_rsl, last_update
    )
    SELECT
        target_cml_id, target_user_id,
        -- 6h window
        ROUND(
            100.0 * COUNT(rsl) FILTER (WHERE time >= now_ts - INTERVAL '6 hours')
                  / NULLIF(COUNT(*) FILTER (WHERE time >= now_ts - INTERVAL '6 hours'), 0),
        2),
        COUNT(*)   FILTER (WHERE time >= now_ts - INTERVAL '6 hours'),
        COUNT(rsl) FILTER (WHERE time >= now_ts - INTERVAL '6 hours'),
        ROUND(AVG(rsl)    FILTER (WHERE time >= now_ts - INTERVAL '6 hours')::numeric, 2),
        ROUND(STDDEV(rsl) FILTER (WHERE time >= now_ts - INTERVAL '6 hours')::numeric, 2),
        -- 1h window
        ROUND(
            100.0 * COUNT(rsl) FILTER (WHERE time >= now_ts - INTERVAL '1 hour')
                  / NULLIF(COUNT(*) FILTER (WHERE time >= now_ts - INTERVAL '1 hour'), 0),
        2),
        ROUND(AVG(rsl)    FILTER (WHERE time >= now_ts - INTERVAL '1 hour')::numeric, 2),
        ROUND(STDDEV(rsl) FILTER (WHERE time >= now_ts - INTERVAL '1 hour')::numeric, 2),
        -- last_rsl: unconstrained so we get the true last RSL even if the CML
        -- has been quiet for more than 6 hours
        (SELECT rsl FROM cml_data
         WHERE  cml_id  = target_cml_id
           AND  user_id = target_user_id
         ORDER  BY time DESC LIMIT 1),
        now_ts
    FROM cml_data
    WHERE cml_id  = target_cml_id
      AND user_id = target_user_id
      AND time   >= now_ts - INTERVAL '6 hours'
    ON CONFLICT (cml_id, user_id) DO UPDATE SET
        completeness_percent_6h = EXCLUDED.completeness_percent_6h,
        total_records_6h        = EXCLUDED.total_records_6h,
        valid_records_6h        = EXCLUDED.valid_records_6h,
        mean_rsl_6h             = EXCLUDED.mean_rsl_6h,
        stddev_rsl_6h           = EXCLUDED.stddev_rsl_6h,
        completeness_percent_1h = EXCLUDED.completeness_percent_1h,
        mean_rsl_1h             = EXCLUDED.mean_rsl_1h,
        stddev_rsl_1h           = EXCLUDED.stddev_rsl_1h,
        last_rsl                = EXCLUDED.last_rsl,
        last_update             = EXCLUDED.last_update;
END;
$$ LANGUAGE plpgsql;

GRANT EXECUTE ON FUNCTION update_cml_stats_windowed(TEXT, TEXT)
    TO demo_openmrg, demo_orange_cameroun;
