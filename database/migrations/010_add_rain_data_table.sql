-- Migration 010: Add cml_rain_data table for processed rain rate estimates
-- This table stores the processed rain rate estimates and intermediate products from CML data

-- Create the main table
CREATE TABLE cml_rain_data (
    time TIMESTAMPTZ NOT NULL,
    cml_id TEXT NOT NULL,
    sublink_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    tl REAL,              -- Total loss (TSL - RSL)
    wet BOOLEAN,          -- Wet/dry classification
    baseline REAL,        -- Baseline attenuation
    waa REAL,             -- Wet antenna attenuation
    a_rain REAL,          -- Rain-induced path attenuation
    r REAL,               -- Rain rate estimate (mm/h)
    PRIMARY KEY (time, cml_id, sublink_id, user_id)
);

-- Convert to hypertable (TimescaleDB)
SELECT create_hypertable('cml_rain_data', 'time');

-- Enable compression (same strategy as cml_data)
ALTER TABLE cml_rain_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'user_id, cml_id'
);

-- Add compression policy (compress chunks older than 7 days)
SELECT add_compression_policy('cml_rain_data', INTERVAL '7 days');

-- Add Row-Level Security (RLS) for multi-user isolation
ALTER TABLE cml_rain_data ENABLE ROW LEVEL SECURITY;

-- Create policy ensuring users only see their own data
CREATE POLICY cml_rain_data_user_policy ON cml_rain_data
    USING (user_id = current_user);

-- Grant permissions to webserver_role for admin access
GRANT SELECT, INSERT ON cml_rain_data TO webserver_role;

-- Create security-barrier view for safe access
CREATE VIEW cml_rain_data_secure WITH (security_barrier) AS
    SELECT * FROM cml_rain_data
    WHERE user_id = current_user;

GRANT SELECT ON cml_rain_data_secure TO webserver_role;
