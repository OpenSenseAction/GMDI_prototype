# Database Migration Guide

---

## PR `feat/db-add-user-id` — Add `user_id` for multi-user RLS support

**Branch:** `feat/db-add-user-id`

`init.sql` only runs on a fresh database volume, so when deploying this branch
to a machine that already has data you must apply the three migration files in
order.

### Changes

| File | What it does |
|------|-------------|
| `migrations/001_add_user_id.sql` | Adds `user_id TEXT NOT NULL DEFAULT 'user1'` to `cml_data`, `cml_metadata`, `cml_stats`; updates primary keys; adds per-user indexes; updates `update_cml_stats` to accept an optional `target_user_id` (DEFAULT `'user1'`) |
| `migrations/002_update_compression_segmentby.sql` | Decompresses existing chunks, adds `user_id` as leading key in `compress_segmentby`, re-compresses old chunks |
| `migrations/003_update_aggregate_user_id.sql` | Drops and recreates `cml_data_1h` with `user_id` in `SELECT` and `GROUP BY` |

### Backward compatibility

All three migrations are **backward-compatible** with the existing single-user
parser:

- `DEFAULT 'user1'` on each `user_id` column means un-modified `INSERT`
  statements (no `user_id` column supplied) keep writing to `user1`.
- A `UNIQUE (cml_id, sublink_id)` constraint is kept on `cml_metadata` so the
  parser's `ON CONFLICT (cml_id, sublink_id)` clause stays valid.
  *(This constraint is dropped in PR `feat/parser-user-id`.)*
- `update_cml_stats(cml_id)` — the existing single-argument call — still works
  because `target_user_id` defaults to `'user1'`.

### Steps

**1. Back up the database**

```bash
docker compose exec database pg_dump -U myuser -d mydatabase \
    > backup_pre_multiuser_$(date +%Y%m%d_%H%M%S).sql
```

**2. Pull and rebuild**

```bash
git pull origin feat/db-add-user-id   # or merge to main first
docker compose up -d --build
```

**3. Apply the migrations in order**

```bash
docker compose exec -T database psql -U myuser -d mydatabase \
    < database/migrations/001_add_user_id.sql

docker compose exec -T database psql -U myuser -d mydatabase \
    < database/migrations/002_update_compression_segmentby.sql

docker compose exec -T database psql -U myuser -d mydatabase \
    < database/migrations/003_update_aggregate_user_id.sql
```

**4. (Optional) Trigger an immediate aggregate backfill**

The refresh policy will backfill `cml_data_1h` within the next hour.
To do it immediately:

```bash
docker compose exec database psql -U myuser -d mydatabase -c \
    "CALL refresh_continuous_aggregate('cml_data_1h', NULL, NULL);"
```

**Rollback:** restore from the backup taken in step 1.

---

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
