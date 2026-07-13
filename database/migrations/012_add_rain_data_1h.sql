-- Migration 012: Add continuous aggregate for rain data (1-hour buckets)
-- Similar to cml_data_1h, this provides efficient querying for Grafana dashboards

-- Create 1-hour continuous aggregate for rain data
CREATE MATERIALIZED VIEW cml_rain_data_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    user_id,
    cml_id,
    sublink_id,
    MIN(r) AS r_min,
    MAX(r) AS r_max,
    AVG(r) AS r_avg,
    MIN(tl) AS tl_min,
    MAX(tl) AS tl_max,
    AVG(tl) AS tl_avg,
    MIN(a_rain) AS a_rain_min,
    MAX(a_rain) AS a_rain_max,
    AVG(a_rain) AS a_rain_avg
FROM cml_rain_data
GROUP BY bucket, user_id, cml_id, sublink_id
WITH NO DATA;

-- Automatically refresh every hour, covering up to 2 days of history
SELECT add_continuous_aggregate_policy('cml_rain_data_1h',
    start_offset      => INTERVAL '2 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);

-- Enable compression for chunks older than 7 days
ALTER TABLE cml_rain_data_1h SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'user_id, cml_id',
    timescaledb.compress_orderby = 'bucket'
);

SELECT add_compression_policy('cml_rain_data_1h', INTERVAL '7 days');

-- Enable Row-Level Security
ALTER TABLE cml_rain_data_1h ENABLE ROW LEVEL SECURITY;

-- RLS policy for per-user isolation
CREATE POLICY cml_rain_data_1h_user_policy ON cml_rain_data_1h
    USING (user_id = current_user);

-- Grant permissions
GRANT SELECT ON cml_rain_data_1h TO webserver_role;

-- Create security-barrier view for safe access
CREATE VIEW cml_rain_data_1h_secure WITH (security_barrier) AS
    SELECT * FROM cml_rain_data_1h
    WHERE user_id = current_user;

GRANT SELECT ON cml_rain_data_1h_secure TO webserver_role;
