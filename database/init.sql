CREATE TABLE cml_data (
    time TIMESTAMPTZ NOT NULL,
    cml_id TEXT NOT NULL,
    rsl REAL,
    tsl REAL
);

SELECT create_hypertable('cml_data', 'time');