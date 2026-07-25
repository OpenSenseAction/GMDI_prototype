-- Migration 013: covering index for open-chunk per-CML raw queries.
--
-- Rationale: the open chunk is always uncompressed.  A per-CML 2-day raw
-- query on it does an index scan on idx_cml_data_user_cml_time to find the
-- matching rows, then fetches each row from the heap.  Because rows from all
-- CMLs are interleaved in insertion order the heap pages are scattered
-- (~6,000 heap reads for ~6,900 rows → ~13 s measured on a large tenant's
-- open chunk).
--
-- INCLUDE carries the payload columns (sublink_id, rsl, tsl) so the planner
-- can satisfy the query entirely from the index (index-only scan), eliminating
-- the heap fetch.  This permanently fixes open-chunk raw query performance
-- without requiring compression.
--
-- NOTE: CREATE INDEX CONCURRENTLY is not supported on TimescaleDB hypertables.
-- timescaledb.transaction_per_chunk locks only one chunk at a time, leaving all
-- other chunks readable and writable throughout the build.  This is the
-- TimescaleDB-recommended equivalent of CONCURRENTLY.
-- If the build is interrupted mid-way, run:
--   SELECT * FROM pg_index WHERE indisvalid IS FALSE;
-- and DROP + recreate the index if any chunk-level indexes are marked invalid.
--
-- Apply with:
--   docker compose exec -T database psql -U myuser -d mydatabase \
--     < database/migrations/013_add_cml_data_covering_index.sql

CREATE INDEX IF NOT EXISTS idx_cml_data_covering
    ON cml_data (user_id, cml_id, time DESC)
    INCLUDE (sublink_id, rsl, tsl)
    WITH (timescaledb.transaction_per_chunk);
