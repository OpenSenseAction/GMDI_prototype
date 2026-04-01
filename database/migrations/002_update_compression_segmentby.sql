-- Migration 002: Update cml_data compression segmentby to include user_id
--
-- Part of PR feat/db-add-user-id.
-- Run this AFTER 001_add_user_id.sql.
--
-- Adds user_id as the leading segmentby key so that per-user range scans
-- decompress only the relevant segments instead of the full chunk.
-- sublink_id is included here alongside cml_id; it was later dropped in
-- migration 005 (feat/db-roles-rls) — see that file for the rationale.
-- The decompress → alter → recompress cycle is non-destructive; no data
-- is lost if the process is interrupted (TimescaleDB keeps the original
-- uncompressed chunks until recompression succeeds).
--
-- Apply with:
--   docker compose exec database psql -U myuser -d mydatabase \
--     -f /path/to/002_update_compression_segmentby.sql

-- Step 1: Decompress all currently-compressed chunks so that the
--         compress_segmentby setting can be changed.
--         Uses timescaledb_information.chunks (works across all TimescaleDB versions).
SELECT decompress_chunk(
    format('%I.%I', chunk_schema, chunk_name)::regclass
)
FROM timescaledb_information.chunks
WHERE hypertable_name = 'cml_data'
  AND is_compressed = true;

-- Step 2: Update the compression settings to include user_id as the
--         leading segment key.  user_id first ensures that a query for a
--         single user decompresses only their segments.
ALTER TABLE cml_data SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'user_id, cml_id, sublink_id',
    timescaledb.compress_orderby   = 'time DESC'
);

-- Step 3: Re-compress chunks that were already old enough for compression
--         (i.e. older than 7 days per the existing policy).
SELECT compress_chunk(c)
FROM   show_chunks('cml_data', older_than => INTERVAL '7 days') c;
