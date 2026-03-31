CREATE TABLE cml_data (
    time TIMESTAMPTZ NOT NULL,
    cml_id TEXT NOT NULL,
    sublink_id TEXT NOT NULL,
    rsl REAL,
    tsl REAL,
    user_id TEXT NOT NULL DEFAULT 'user1'
);

CREATE TABLE cml_metadata (
    cml_id TEXT NOT NULL,
    sublink_id TEXT NOT NULL,
    site_0_lon REAL,
    site_0_lat REAL,
    site_1_lon REAL,
    site_1_lat REAL,
    frequency REAL,
    polarization TEXT,
    length REAL,
    user_id TEXT NOT NULL DEFAULT 'user1',
    PRIMARY KEY (cml_id, sublink_id, user_id),
    -- Backward-compat constraint: keeps the parser's ON CONFLICT (cml_id, sublink_id)
    -- clause valid until PR3 (feat/parser-user-id) updates it.
    UNIQUE (cml_id, sublink_id)
);

CREATE TABLE cml_stats (
    cml_id TEXT NOT NULL,
    user_id TEXT NOT NULL DEFAULT 'user1',
    total_records BIGINT,
    valid_records BIGINT,
    null_records BIGINT,
    completeness_percent REAL,
    min_rsl REAL,
    max_rsl REAL,
    mean_rsl REAL,
    stddev_rsl REAL,
    last_rsl REAL,
    last_update TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (cml_id, user_id)
);

-- update_cml_stats(target_cml_id, target_user_id)
--
-- target_user_id defaults to 'user1' so the existing single-argument call
-- sites in the parser continue to work until PR3 updates them.
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

SELECT create_hypertable('cml_data', 'time');

-- Per-user lookup indexes.
CREATE INDEX idx_cml_data_user_id     ON cml_data     (user_id);
CREATE INDEX idx_cml_metadata_user_id ON cml_metadata  (user_id);

-- Index is created by the archive_loader service after bulk data load (faster COPY).
-- If no archive data is loaded, create it manually:
-- CREATE INDEX idx_cml_data_cml_id ON cml_data (cml_id, time DESC);

-- ---------------------------------------------------------------------------
-- 1-hour continuous aggregate for fast queries over large time ranges.
-- Grafana and the webserver automatically switch to this view when the
-- requested time range exceeds 3 days, reducing the scanned row count
-- by ~360x (10-second raw data → 1-hour buckets).
-- ---------------------------------------------------------------------------
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

-- Automatically refresh every hour, covering up to 2 days of history.
-- The 1-hour end_offset prevents partial (in-progress) buckets from being
-- materialised prematurely; very recent data reads through to raw cml_data.
SELECT add_continuous_aggregate_policy('cml_data_1h',
    start_offset      => INTERVAL '2 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);

-- ---------------------------------------------------------------------------
-- Compression for cml_data chunks older than 7 days.
--
-- compress_segmentby: each compressed segment contains one (cml_id, sublink_id)
--   pair, so a query filtered to a single CML decompresses only ~1/728th of a
--   chunk — not the whole thing.
-- compress_orderby: matches the query pattern (time range scans), allowing
--   skip-scan decompression for narrow time windows within a segment.
--
-- At ~10-20x compression ratio, the last month of data fits in shared_buffers
-- after a single cache warm-up, regardless of how many new streams are added.
-- The current uncompressed week chunk is left untouched so real-time ingestion
-- and detail-view queries on recent data have no decompression overhead.
-- ---------------------------------------------------------------------------
ALTER TABLE cml_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'user_id, cml_id, sublink_id',
    timescaledb.compress_orderby   = 'time DESC'
);

SELECT add_compression_policy('cml_data', INTERVAL '7 days');

-- ---------------------------------------------------------------------------
-- Database roles and Row-Level Security (PR feat/db-roles-rls)
--
-- user1_role: used by the user1 parser instance (writes) and by the
--   webserver (reads via SET ROLE) for user1's scoped data.
-- webserver_role: used by the webserver process.  Has a read-all RLS policy
--   for aggregate/admin queries; SET ROLEs to a user role for scoped reads.
--
-- Passwords shown here are development defaults.
-- Override them via environment variables or a secrets manager in production.
--
-- Note on cml_data_1h:
--   PostgreSQL RLS cannot be applied to materialized views, so queries to
--   cml_data_1h MUST include a WHERE user_id = ? predicate at the
--   application layer.  All raw-data queries route through the RLS-protected
--   base table (cml_data) and are automatically filtered.
-- ---------------------------------------------------------------------------

CREATE ROLE user1_role    LOGIN PASSWORD 'user1password';
CREATE ROLE webserver_role LOGIN PASSWORD 'webserverpassword';

-- Allow webserver_role to impersonate user roles (SET ROLE user1_role).
GRANT user1_role TO webserver_role;

-- Schema access.
GRANT USAGE ON SCHEMA public TO user1_role, webserver_role;

-- Table permissions.
GRANT SELECT, INSERT, UPDATE ON cml_data     TO user1_role;
GRANT SELECT, INSERT, UPDATE ON cml_metadata TO user1_role;
GRANT SELECT, INSERT, UPDATE ON cml_stats    TO user1_role;

GRANT SELECT ON cml_data     TO webserver_role;
GRANT SELECT ON cml_metadata TO webserver_role;
GRANT SELECT ON cml_stats    TO webserver_role;

-- Continuous aggregate — application must add WHERE user_id = ? filter.
GRANT SELECT ON cml_data_1h TO user1_role, webserver_role;

-- Parser calls update_cml_stats() to upsert per-CML statistics.
GRANT EXECUTE ON FUNCTION update_cml_stats(TEXT, TEXT) TO user1_role;

-- Enable Row-Level Security on base tables.
ALTER TABLE cml_data     ENABLE ROW LEVEL SECURITY;
ALTER TABLE cml_metadata ENABLE ROW LEVEL SECURITY;
ALTER TABLE cml_stats    ENABLE ROW LEVEL SECURITY;

-- RLS policies for user1_role.
CREATE POLICY user1_cml_data_policy ON cml_data
    FOR ALL TO user1_role
    USING     (user_id = 'user1')
    WITH CHECK (user_id = 'user1');

CREATE POLICY user1_cml_metadata_policy ON cml_metadata
    FOR ALL TO user1_role
    USING     (user_id = 'user1')
    WITH CHECK (user_id = 'user1');

CREATE POLICY user1_cml_stats_policy ON cml_stats
    FOR ALL TO user1_role
    USING     (user_id = 'user1')
    WITH CHECK (user_id = 'user1');

-- RLS policies for webserver_role (read-all; scoped reads use SET ROLE).
CREATE POLICY webserver_cml_data_policy ON cml_data
    FOR SELECT TO webserver_role
    USING (true);

CREATE POLICY webserver_cml_metadata_policy ON cml_metadata
    FOR SELECT TO webserver_role
    USING (true);

CREATE POLICY webserver_cml_stats_policy ON cml_stats
    FOR SELECT TO webserver_role
    USING (true);