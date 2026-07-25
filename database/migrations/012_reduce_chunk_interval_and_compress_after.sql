-- Migration 012: 1-day chunks + 1-day compress_after for cml_data.
--
-- Rationale: bound the always-uncompressed open chunk so per-CML 2-day raw
-- queries stay fast regardless of tenant size.  A large tenant (~12k sublinks
-- @ 10 s ≈ 9 GB/day) otherwise grows a 60 GB open chunk (7-day interval) that
-- makes every short raw query a massive scattered-heap-read problem.
-- With 1-day chunks:
--   - The open chunk stays ≤ 9 GB for the largest tenant.
--   - Yesterday's closed chunk is compressed within ~1 day, so most data is
--     already in fast columnar storage.
--   - A "whole day" query maps to exactly one chunk.
--
-- IMPORTANT: set_chunk_time_interval affects only chunks CREATED AFTER this
-- runs.  Existing 7-day chunks are unchanged (no data migration needed).
--
-- Apply with:
--   docker compose exec -T database psql -U myuser -d mydatabase \
--     < database/migrations/012_reduce_chunk_interval_and_compress_after.sql

SELECT set_chunk_time_interval('cml_data', INTERVAL '1 day');

-- Re-point the existing compression policy to compress_after = 1 day.
-- The job_id is looked up dynamically so this is safe across environments
-- (do not hard-code job_id=1002).
DO $$
DECLARE
    v_job_id INTEGER;
BEGIN
    SELECT job_id INTO v_job_id
    FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_compression'
      AND hypertable_name = 'cml_data';

    IF v_job_id IS NULL THEN
        RAISE EXCEPTION 'No compression policy found for cml_data. Check timescaledb_information.jobs.';
    END IF;

    PERFORM alter_job(v_job_id,
        config => jsonb_set(
            (SELECT config FROM timescaledb_information.jobs WHERE job_id = v_job_id),
            '{compress_after}', '"1 day"'));
END $$;
