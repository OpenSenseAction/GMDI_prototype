-- Migration 003: Recreate cml_data_1h continuous aggregate with user_id
--
-- Part of PR feat/db-add-user-id.
-- Run this AFTER 001_add_user_id.sql.  Order relative to 002 is not critical.
--
-- The existing cml_data_1h view does not include user_id in its GROUP BY,
-- so RLS policies (added in PR2) cannot be applied to it.  The view must be
-- dropped and recreated; this is a non-destructive operation because the
-- continuous aggregate is re-materialised from the underlying raw cml_data.
--
-- A brief gap in Grafana's hourly-aggregate data is expected while the
-- refresh policy backfills the view (~1 refresh cycle, up to 1 hour).
-- Queries that fall in the gap automatically fall through to raw cml_data.
--
-- Apply with:
--   docker compose exec database psql -U myuser -d mydatabase \
--     -f /path/to/003_update_aggregate_user_id.sql

-- Step 1: Remove the old view and its dependent policy + grants.
DROP MATERIALIZED VIEW IF EXISTS cml_data_1h CASCADE;

-- Step 2: Recreate with user_id in SELECT and GROUP BY.
CREATE MATERIALIZED VIEW cml_data_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    user_id,
    cml_id,
    sublink_id,
    MIN(rsl)  AS rsl_min,
    MAX(rsl)  AS rsl_max,
    AVG(rsl)  AS rsl_avg,
    MIN(tsl)  AS tsl_min,
    MAX(tsl)  AS tsl_max,
    AVG(tsl)  AS tsl_avg
FROM cml_data
GROUP BY bucket, user_id, cml_id, sublink_id
WITH NO DATA;

-- Step 3: Restore the refresh policy (same parameters as before).
SELECT add_continuous_aggregate_policy('cml_data_1h',
    start_offset      => INTERVAL '2 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);

-- Step 4: Optional — trigger an immediate backfill rather than waiting for
--         the next scheduled refresh.  Remove the leading '--' to enable.
-- CALL refresh_continuous_aggregate('cml_data_1h', NULL, NULL);
