-- Migration 006: Add user2 role and finalize parser user_id wiring
--
-- Part of PR feat/sftp-multi-user (onboarding a second SFTP user).
-- Run this AFTER migrations 001–005 have been applied.
--
-- What this migration does:
--   1. Creates the user2 login role (parser + webserver for user2's data).
--   2. Grants table/function/view permissions to user2 — identical pattern
--      to user1 because the generic current_user-based RLS policies and
--      security-barrier views automatically cover any role whose name
--      matches its user_id value.
--   3. Grants user2 to webserver_role so the webserver can SET ROLE user2
--      for DB-enforced scoped reads.
--   4. Drops the backward-compatibility UNIQUE (cml_id, sublink_id)
--      constraint on cml_metadata.  That constraint was kept from migration
--      001 to let the old single-argument INSERT ... ON CONFLICT clause
--      continue to work until the parser was updated (PR3 / feat/parser-user-id).
--      Now that the parser includes user_id in every INSERT and targets the
--      (cml_id, sublink_id, user_id) primary key for ON CONFLICT, the old
--      constraint is no longer needed and would prevent two different users
--      from uploading data for the same CML link identifiers.
--
-- Onboarding additional users later:
--   Repeat steps 1–3 with user3, user4, …  No new RLS policies are needed;
--   the generic current_user policy covers every role automatically.
--
-- Apply with:
--   docker compose exec -T database psql -U myuser -d mydatabase \
--     < database/migrations/006_add_user2.sql
--
-- Rollback: revoke grants, drop the role.  The UNIQUE constraint cannot be
--   restored without re-adding it manually (see MIGRATION.md).

-- ---------------------------------------------------------------------------
-- Step 1: Create user2 login role (idempotent)
-- ---------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'user2') THEN
        CREATE ROLE user2 LOGIN PASSWORD 'user2password';
    END IF;
END
$$;

-- ---------------------------------------------------------------------------
-- Step 2: Schema access
-- ---------------------------------------------------------------------------

GRANT USAGE ON SCHEMA public TO user2;

-- ---------------------------------------------------------------------------
-- Step 3: Table permissions
-- ---------------------------------------------------------------------------

GRANT SELECT, INSERT, UPDATE ON cml_data     TO user2;
GRANT SELECT, INSERT, UPDATE ON cml_metadata TO user2;
GRANT SELECT, INSERT, UPDATE ON cml_stats    TO user2;

GRANT EXECUTE ON FUNCTION update_cml_stats(TEXT, TEXT) TO user2;

-- ---------------------------------------------------------------------------
-- Step 4: Security-barrier view access
-- ---------------------------------------------------------------------------

GRANT SELECT ON cml_data_secure     TO user2;
GRANT SELECT ON cml_data_1h_secure  TO user2;

-- ---------------------------------------------------------------------------
-- Step 5: Allow webserver_role to impersonate user2 (SET ROLE user2)
-- ---------------------------------------------------------------------------

GRANT user2 TO webserver_role;

-- ---------------------------------------------------------------------------
-- Step 6: Drop the backward-compat single-column unique constraint on
--         cml_metadata that was kept from migration 001.
--
--         The parser now inserts with explicit user_id and resolves conflicts
--         on the (cml_id, sublink_id, user_id) primary key, so this
--         constraint is both unnecessary and harmful for multi-user data.
-- ---------------------------------------------------------------------------

ALTER TABLE cml_metadata
    DROP CONSTRAINT IF EXISTS cml_metadata_sublink_unique;
