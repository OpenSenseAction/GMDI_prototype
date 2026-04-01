-- Migration 005: Drop sublink_id from cml_data compression segmentby
--
-- Part of PR feat/db-roles-rls.
-- Run this AFTER migration 004_add_roles_rls.sql.
--
-- Rationale:
--   Migration 002 set compress_segmentby = 'user_id, cml_id, sublink_id'.
--   Keeping sublink_id as a segment key means a query for one CML
--   requires decompressing one segment per sublink.  Given the expected
--   data distribution (~80% of CMLs have 2 sublinks, ~15% have 4),
--   removing sublink_id reduces average decompression work per CML query
--   by roughly 2–4×.  Filtering to a specific sublink after decompression
--   is a trivial CPU operation on already-decompressed columnar data, so
--   there is no meaningful cost on that side.
--
--   Sublinks of the same CML share similar RSL/TSL value ranges (same
--   physical link), so they continue to compress well together within one
--   (user_id, cml_id) segment.
--
-- The decompress → alter → recompress cycle is non-destructive; no data
-- is lost if the process is interrupted (TimescaleDB keeps the original
-- uncompressed chunks until recompression succeeds).
--
-- Apply with:
--   docker compose exec -T database psql -U myuser -d mydatabase \
--     < database/migrations/005_drop_sublink_from_segmentby.sql

-- Step 1: Decompress all currently-compressed chunks.
SELECT decompress_chunk(
    format('%I.%I', chunk_schema, chunk_name)::regclass
)
FROM timescaledb_information.chunks
WHERE hypertable_name = 'cml_data'
  AND is_compressed = true;

-- Step 2: Update compress_segmentby to (user_id, cml_id) — drop sublink_id.
ALTER TABLE cml_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'user_id, cml_id',
    timescaledb.compress_orderby   = 'time DESC'
);

-- Step 3: Re-compress chunks older than the policy threshold (7 days).
SELECT compress_chunk(c)
FROM   show_chunks('cml_data', older_than => INTERVAL '7 days') c;
