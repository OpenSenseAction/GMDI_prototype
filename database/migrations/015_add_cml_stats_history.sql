-- Migration 015: Add cml_stats_history hypertable for historical time slider
--
-- This migration adds a new hypertable to store snapshots of CML stats at hourly intervals.
-- Enables the historical time slider feature on the realtime map.
--
-- Key features:
-- - Stores hourly snapshots of cml_stats for historical queries
-- - Supports provisional (current partial hour) and definitive (completed hours) rows
-- - Compression policy for old data (>7 days)
-- - Functions for materialization and querying
--
-- Depends on migration 014_add_record_count_to_cml_data_1h.sql, which adds
-- the record_count column that materialize_cml_stats_snapshot() reads below.
--
-- Apply with:
--   docker compose exec -T database psql -U myuser -d mydatabase \
--     < database/migrations/015_add_cml_stats_history.sql

-- Create the cml_stats_history table
CREATE TABLE IF NOT EXISTS cml_stats_history (
    snapshot_time             TIMESTAMPTZ NOT NULL,
    cml_id                    TEXT        NOT NULL,
    user_id                   TEXT        NOT NULL,
    completeness_percent_6h   REAL,
    total_records_6h          BIGINT,
    valid_records_6h          BIGINT,
    mean_rsl_6h               REAL,
    stddev_rsl_6h             REAL,
    completeness_percent_1h   REAL,
    mean_rsl_1h               REAL,
    stddev_rsl_1h             REAL,
    last_rsl                  REAL,
    is_provisional            BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (snapshot_time, cml_id, user_id)
);

-- Create hypertable with 7-day chunks
SELECT create_hypertable(
    'cml_stats_history', 'snapshot_time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Enable compression for old snapshots (>7 days)
ALTER TABLE cml_stats_history
    SET (timescaledb.compress,
         timescaledb.compress_segmentby = 'user_id, cml_id',
         timescaledb.compress_orderby   = 'snapshot_time DESC');

SELECT add_compression_policy('cml_stats_history', INTERVAL '7 days');

-- Materialization function: writes definitive snapshot from cml_data_1h
CREATE OR REPLACE FUNCTION materialize_cml_stats_snapshot(
    p_at_time   TIMESTAMPTZ,   -- must be hour-truncated by caller
    p_user_id   TEXT
) RETURNS INT                  -- number of rows upserted
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_rows INT;
BEGIN
    -- Validate caller is a known user
    IF NOT EXISTS (SELECT 1 FROM cml_metadata WHERE user_id = p_user_id LIMIT 1) THEN
        RAISE EXCEPTION 'Unknown user: %', p_user_id;
    END IF;

    INSERT INTO cml_stats_history (
        snapshot_time, cml_id, user_id,
        completeness_percent_6h, total_records_6h, valid_records_6h,
        mean_rsl_6h, stddev_rsl_6h,
        completeness_percent_1h, mean_rsl_1h, stddev_rsl_1h,
        last_rsl, is_provisional
    )
    SELECT
        p_at_time,
        m.cml_id,
        p_user_id,
        -- 6-hour window: sum the 6 hourly buckets ending at p_at_time
        -- (h6 already aggregates across those buckets internally, so no
        -- further SUM/AVG or GROUP BY is needed at this level)
        ROUND(
            100.0 * h6.rsl_count::numeric
                  / NULLIF(h6.record_count, 0),
            2) AS completeness_percent_6h,
        h6.record_count                          AS total_records_6h,
        h6.rsl_count                              AS valid_records_6h,
        ROUND(h6.rsl_avg::numeric, 2)            AS mean_rsl_6h,
        ROUND(h6.rsl_stddev::numeric, 2)         AS stddev_rsl_6h,
        -- 1-hour window: the single bucket containing p_at_time
        ROUND(
            100.0 * h1.rsl_count::numeric
                  / NULLIF(h1.record_count, 0),
            2) AS completeness_percent_1h,
        ROUND(h1.rsl_avg::numeric, 2)            AS mean_rsl_1h,
        ROUND(h1.rsl_stddev::numeric, 2)         AS stddev_rsl_1h,
        h1.rsl_max                               AS last_rsl,
        FALSE                                    AS is_provisional
    FROM cml_metadata m
    -- 6h subquery from the 1h continuous aggregate
    LEFT JOIN LATERAL (
        SELECT
            SUM(record_count)                          AS record_count,
            SUM(CASE WHEN rsl_max IS NOT NULL THEN record_count ELSE 0 END) AS rsl_count,
            AVG(rsl_avg)                               AS rsl_avg,
            AVG(rsl_max - rsl_min)                     AS rsl_stddev
        FROM cml_data_1h
        WHERE user_id    = p_user_id
          AND cml_id     = m.cml_id
          AND bucket     >= p_at_time - INTERVAL '6 hours'
          AND bucket     <  p_at_time
    ) h6 ON TRUE
    -- 1h subquery: the bucket immediately before p_at_time
    LEFT JOIN LATERAL (
        SELECT record_count, rsl_avg, rsl_max, rsl_min,
               CASE WHEN rsl_max IS NOT NULL THEN record_count ELSE 0 END AS rsl_count,
               (rsl_max - rsl_min) AS rsl_stddev
        FROM cml_data_1h
        WHERE user_id = p_user_id
          AND cml_id  = m.cml_id
          AND bucket  = p_at_time - INTERVAL '1 hour'
    ) h1 ON TRUE
    WHERE m.user_id = p_user_id
    ON CONFLICT (snapshot_time, cml_id, user_id) DO UPDATE SET
        completeness_percent_6h = EXCLUDED.completeness_percent_6h,
        total_records_6h        = EXCLUDED.total_records_6h,
        valid_records_6h        = EXCLUDED.valid_records_6h,
        mean_rsl_6h             = EXCLUDED.mean_rsl_6h,
        stddev_rsl_6h           = EXCLUDED.stddev_rsl_6h,
        completeness_percent_1h = EXCLUDED.completeness_percent_1h,
        mean_rsl_1h             = EXCLUDED.mean_rsl_1h,
        stddev_rsl_1h           = EXCLUDED.stddev_rsl_1h,
        last_rsl                = EXCLUDED.last_rsl,
        is_provisional          = FALSE;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows;
END;
$$;

-- Read function for webserver / Grafana
CREATE OR REPLACE FUNCTION get_cml_stats_at(
    p_at_time   TIMESTAMPTZ,
    p_user_id   TEXT
) RETURNS TABLE (
    cml_id                   TEXT,
    completeness_percent_6h  REAL,
    total_records_6h         BIGINT,
    valid_records_6h         BIGINT,
    mean_rsl_6h              REAL,
    stddev_rsl_6h            REAL,
    completeness_percent_1h  REAL,
    mean_rsl_1h              REAL,
    stddev_rsl_1h            REAL,
    last_rsl                 REAL,
    is_provisional           BOOLEAN
) LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public
AS $$
    SELECT
        h.cml_id,
        h.completeness_percent_6h,
        h.total_records_6h,
        h.valid_records_6h,
        h.mean_rsl_6h,
        h.stddev_rsl_6h,
        h.completeness_percent_1h,
        h.mean_rsl_1h,
        h.stddev_rsl_1h,
        h.last_rsl,
        h.is_provisional
    FROM cml_stats_history h
    WHERE h.user_id       = p_user_id
      AND h.snapshot_time = date_trunc('hour', p_at_time)
    ORDER BY h.cml_id;
$$;

-- Grants for writers (parser roles) - applied per-user by generate_config.py
-- GRANT SELECT, INSERT, UPDATE ON cml_stats_history TO <user_id>;
-- GRANT EXECUTE ON FUNCTION materialize_cml_stats_snapshot(TIMESTAMPTZ, TEXT) TO <user_id>;

-- Grants for readers (webserver, Grafana)
GRANT SELECT ON cml_stats_history TO webserver_role;
GRANT EXECUTE ON FUNCTION get_cml_stats_at(TIMESTAMPTZ, TEXT)
    TO webserver_role;
