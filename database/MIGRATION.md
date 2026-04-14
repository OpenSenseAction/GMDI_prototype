# Database Migration Guide

## `cml_data_1h` continuous aggregate

**Branch:** `feature/performance-and-grafana-improvements`

`init.sql` only runs on a fresh database volume, so when deploying this branch
to a machine that already has data you must apply the migration manually.

### Steps

**1. Pull and redeploy the application**

```bash
git pull origin main
docker compose up -d --build
```

**2. Create the continuous aggregate**

```bash
docker compose exec database psql -U myuser -d mydatabase -c "
CREATE MATERIALIZED VIEW cml_data_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    cml_id,
    sublink_id,
    MIN(rsl)  AS rsl_min,
    MAX(rsl)  AS rsl_max,
    AVG(rsl)  AS rsl_avg,
    MIN(tsl)  AS tsl_min,
    MAX(tsl)  AS tsl_max,
    AVG(tsl)  AS tsl_avg
FROM cml_data
GROUP BY bucket, cml_id, sublink_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('cml_data_1h',
    start_offset      => INTERVAL '2 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);
"
```

**3. Backfill historical data (one-time)**

```bash
docker compose exec database psql -U myuser -d mydatabase -c "
CALL refresh_continuous_aggregate('cml_data_1h', NULL, NULL);
"
```

This may take a few seconds depending on how much data is present. After it
completes the refresh policy keeps the view up to date automatically.

---

## Unique constraint on `cml_data` (idempotent ingestion)

**Branch:** `fix/mno-simulator-resilience`

A `UNIQUE (time, cml_id, sublink_id)` constraint was added to `cml_data` so
that re-processing the same file never creates duplicate rows.  The parser's
`write_rawdata` now uses `ON CONFLICT DO NOTHING`.

### Steps

**1. Deduplicate existing data (if any duplicates are present)**

```bash
docker compose exec database psql -U myuser -d mydatabase -c "
DELETE FROM cml_data a
USING cml_data b
WHERE a.ctid < b.ctid
  AND a.time        = b.time
  AND a.cml_id      = b.cml_id
  AND a.sublink_id  = b.sublink_id;
"
```

**2. Add the unique constraint**

```bash
docker compose exec database psql -U myuser -d mydatabase -c "
ALTER TABLE cml_data ADD CONSTRAINT cml_data_unique_obs
    UNIQUE (time, cml_id, sublink_id);
"
```

Note: TimescaleDB requires that unique indexes on hypertables include the
partitioning column (`time`), which is satisfied here.
