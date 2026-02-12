CREATE TABLE cml_data (
    time TIMESTAMPTZ NOT NULL,
    cml_id TEXT NOT NULL,
    sublink_id TEXT NOT NULL,
    rsl REAL,
    tsl REAL
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
    PRIMARY KEY (cml_id, sublink_id)
);

CREATE TABLE cml_stats (
    cml_id TEXT PRIMARY KEY,
    total_records BIGINT,
    valid_records BIGINT,
    null_records BIGINT,
    completeness_percent REAL,
    min_rsl REAL,
    max_rsl REAL,
    mean_rsl REAL,
    stddev_rsl REAL,
    last_rsl REAL,
    last_update TIMESTAMPTZ DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION update_cml_stats(target_cml_id TEXT) RETURNS VOID AS $$
BEGIN
    INSERT INTO cml_stats (
        cml_id,
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
        COUNT(*) as total_records,
        COUNT(CASE WHEN cd.rsl IS NOT NULL THEN 1 END) as valid_records,
        COUNT(CASE WHEN cd.rsl IS NULL THEN 1 END) as null_records,
        ROUND(100.0 * COUNT(CASE WHEN cd.rsl IS NOT NULL THEN 1 END) / COUNT(*), 2) as completeness_percent,
        MIN(cd.rsl) as min_rsl,
        MAX(cd.rsl) as max_rsl,
        ROUND(AVG(cd.rsl)::numeric, 2) as mean_rsl,
        ROUND(STDDEV(cd.rsl)::numeric, 2) as stddev_rsl,
        (SELECT rsl FROM cml_data WHERE cml_id = cd.cml_id ORDER BY time DESC LIMIT 1) as last_rsl,
        NOW()
    FROM cml_data cd
    WHERE cd.cml_id = target_cml_id
    GROUP BY cd.cml_id
    ON CONFLICT (cml_id) DO UPDATE SET
        total_records = EXCLUDED.total_records,
        valid_records = EXCLUDED.valid_records,
        null_records = EXCLUDED.null_records,
        completeness_percent = EXCLUDED.completeness_percent,
        min_rsl = EXCLUDED.min_rsl,
        max_rsl = EXCLUDED.max_rsl,
        mean_rsl = EXCLUDED.mean_rsl,
        stddev_rsl = EXCLUDED.stddev_rsl,
        last_rsl = EXCLUDED.last_rsl,
        last_update = EXCLUDED.last_update;
END;
$$ LANGUAGE plpgsql;

SELECT create_hypertable('cml_data', 'time');