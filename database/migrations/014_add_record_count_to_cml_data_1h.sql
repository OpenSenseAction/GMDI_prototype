-- Migration 014: Add record_count to cml_data_1h continuous aggregate
--
-- Prerequisite for the historical time slider (see the following
-- cml_stats_history migration). materialize_cml_stats_snapshot() computes
-- completeness_percent_6h / completeness_percent_1h from the number of raw
-- samples in each hourly bucket, which requires a record_count column that
-- cml_data_1h has never had (it was only ever needed for Grafana's raw
-- RSL/TSL line charts, which use MIN/MAX/AVG, not counts).
--
-- Continuous aggregates cannot have a column added via ALTER; the view must
-- be dropped and recreated. This is non-destructive: TimescaleDB
-- re-materialises from the underlying raw cml_data, matching the pattern
-- already used in migration 003_update_aggregate_user_id.sql.
--
-- Dropping cml_data_1h CASCADEs to its continuous aggregate policy and to
-- the cml_data_1h_secure security-barrier view (and that view's grants), so
-- all three must be recreated here.
--
-- A brief gap in Grafana's hourly-aggregate data is expected while the
-- refresh policy backfills the view (~1 refresh cycle, up to 1 hour).
-- Queries that fall in the gap automatically fall through to raw cml_data.
--
-- Apply with:
--   docker compose exec -T database psql -U myuser -d mydatabase \
--     < database/migrations/014_add_record_count_to_cml_data_1h.sql

-- Step 1: Remove the old view and its dependent policy, secure view, and grants.
DROP MATERIALIZED VIEW IF EXISTS cml_data_1h CASCADE;

-- Step 2: Recreate with record_count added.
CREATE MATERIALIZED VIEW cml_data_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    user_id,
    cml_id,
    sublink_id,
    MIN(rsl)     AS rsl_min,
    MAX(rsl)     AS rsl_max,
    AVG(rsl)     AS rsl_avg,
    MIN(tsl)     AS tsl_min,
    MAX(tsl)     AS tsl_max,
    AVG(tsl)     AS tsl_avg,
    COUNT(*)     AS record_count
FROM cml_data
GROUP BY bucket, user_id, cml_id, sublink_id
WITH NO DATA;

-- Step 3: Restore the refresh policy (same parameters as before).
SELECT add_continuous_aggregate_policy('cml_data_1h',
    start_offset      => INTERVAL '2 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);

-- Step 4: Recreate the security-barrier view dropped by CASCADE above.
CREATE VIEW cml_data_1h_secure WITH (security_barrier) AS
SELECT * FROM cml_data_1h
WHERE user_id = current_user;

-- Step 5: Restore grants dropped by CASCADE above.
GRANT SELECT ON cml_data_1h_secure TO demo_openmrg, demo_orange_cameroun;
GRANT SELECT ON cml_data_1h        TO webserver_role;
GRANT SELECT ON cml_data_1h_secure TO webserver_role;

-- Step 6: Optional — trigger an immediate backfill rather than waiting for
--         the next scheduled refresh.  Remove the leading '--' to enable.
-- CALL refresh_continuous_aggregate('cml_data_1h', NULL, NULL);
