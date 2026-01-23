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

SELECT create_hypertable('cml_data', 'time');