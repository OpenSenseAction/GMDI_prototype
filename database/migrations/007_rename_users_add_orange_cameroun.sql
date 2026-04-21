-- Migration 007: Rename user1/user2 to demo_openmrg/demo_orange_cameroun
--
-- What this migration does:
--   1. Renames user1 → demo_openmrg  (the OpenMRG dataset simulator user)
--   2. Renames user2 → demo_orange_cameroun  (the Orange Cameroun dataset user)
--
-- The generic current_user-based RLS policies on cml_metadata and cml_stats
-- automatically cover the renamed roles because they compare user_id to
-- current_user — no policy changes are needed.
--
-- The cml_data_secure and cml_data_1h_secure security-barrier views also
-- require no changes (they filter WHERE user_id = current_user).
--
-- The cml_data, cml_metadata and cml_stats tables store user_id as TEXT.
-- Existing rows written by user1/user2 must also be relabelled so that the
-- renamed roles can still read and write their own data under RLS.
--
-- Apply with:
--   docker compose exec -T database psql -U myuser -d mydatabase \
--     < database/migrations/007_rename_users_add_orange_cameroun.sql
--
-- Rollback: reverse the ALTER ROLE RENAME and UPDATE statements.

-- ---------------------------------------------------------------------------
-- Step 1: Rename DB roles
-- ---------------------------------------------------------------------------

DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'user1') THEN
        ALTER ROLE user1 RENAME TO demo_openmrg;
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'user2') THEN
        ALTER ROLE user2 RENAME TO demo_orange_cameroun;
    END IF;
END
$$;

-- ---------------------------------------------------------------------------
-- Step 2: Relabel existing data rows
--
-- cml_metadata and cml_stats are small uncompressed tables — fast to UPDATE.
--
-- cml_data is a compressed TimescaleDB hypertable. UPDATE forces decompression
-- of every chunk, which on months of 10-second-resolution data can take many
-- minutes and temporarily double storage.  Since Grafana connects as the
-- superuser (myuser) and bypasses RLS, old rows labelled user_id='user1' remain
-- fully visible in dashboards.  New rows written by the renamed parser will
-- already carry user_id='demo_openmrg', so the relabelling converges naturally
-- over time as old compressed chunks age out.
--
-- To force a full relabel of cml_data (optional, e.g. before enabling strict
-- RLS enforcement for parsers), run the following AFTER decompressing all chunks:
--
--   SELECT decompress_chunk(c) FROM show_chunks('cml_data') c;
--   UPDATE cml_data SET user_id = 'demo_openmrg' WHERE user_id = 'user1';
--   UPDATE cml_data SET user_id = 'demo_orange_cameroun' WHERE user_id = 'user2';
--   SELECT compress_chunk(c) FROM show_chunks('cml_data') c;
-- ---------------------------------------------------------------------------

UPDATE cml_metadata SET user_id = 'demo_openmrg'         WHERE user_id = 'user1';
UPDATE cml_stats    SET user_id = 'demo_openmrg'         WHERE user_id = 'user1';

UPDATE cml_metadata SET user_id = 'demo_orange_cameroun' WHERE user_id = 'user2';
UPDATE cml_stats    SET user_id = 'demo_orange_cameroun' WHERE user_id = 'user2';
