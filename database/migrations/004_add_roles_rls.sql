-- Migration 004: Create database roles and enable Row-Level Security
--
-- Part of PR feat/db-roles-rls (multi-user RLS support, Phase 2).
-- Run this AFTER migrations 001–003 (feat/db-add-user-id).
--
-- What this migration does:
--   1. Creates user1_role (parser + webserver for user1's data).
--   2. Creates webserver_role (read-all for admin/aggregate queries;
--      can SET ROLE to a user role for scoped reads).
--   3. Grants table and function permissions to each role.
--   4. Enables Row-Level Security on cml_data, cml_metadata, cml_stats.
--   5. Creates per-role RLS policies on those three tables.
--   6. Grants SELECT on cml_data_1h (continuous aggregate).
--
-- Backward-compatibility:
--   myuser (superuser) bypasses RLS by default, so the existing parser
--   and webserver — which both still connect as myuser — continue to work
--   without any changes until PR3 (feat/parser-user-id) and
--   PR5 (feat/webserver-auth) wire up the new role credentials.
--
-- Note on cml_data_1h (continuous aggregate):
--   PostgreSQL RLS cannot be applied to materialized views, so cml_data_1h
--   has no automatic row filtering.  Queries to this view MUST always include
--   a WHERE user_id = ? predicate.  The webserver (PR5) and Grafana enforce
--   this at the application layer.  All raw-data queries go through the
--   RLS-protected base table (cml_data) and ARE automatically filtered.
--
-- Passwords shown here are development defaults.  Override them via
-- environment variables or a secrets manager before going to production.
--
-- Apply with:
--   docker compose exec -T database psql -U myuser -d mydatabase \
--     < database/migrations/004_add_roles_rls.sql
--
-- Rollback: see MIGRATION.md — drop the roles after revoking all grants.

-- ---------------------------------------------------------------------------
-- Step 1: Create roles (idempotent via DO block)
-- ---------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'user1_role') THEN
        CREATE ROLE user1_role LOGIN PASSWORD 'user1password';
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'webserver_role') THEN
        CREATE ROLE webserver_role LOGIN PASSWORD 'webserverpassword';
    END IF;
END
$$;

-- Allow webserver_role to impersonate user roles (SET ROLE user1_role).
-- This replaces the connection-level role switch so the webserver can scope
-- all queries to the logged-in user's data without reconnecting.
GRANT user1_role TO webserver_role;

-- ---------------------------------------------------------------------------
-- Step 2: Schema access
-- ---------------------------------------------------------------------------

GRANT USAGE ON SCHEMA public TO user1_role, webserver_role;

-- ---------------------------------------------------------------------------
-- Step 3: Table permissions
-- ---------------------------------------------------------------------------

-- user1_role: INSERT/SELECT/UPDATE on the three data tables.
-- No DELETE: raw data is never deleted by design.
GRANT SELECT, INSERT, UPDATE ON cml_data      TO user1_role;
GRANT SELECT, INSERT, UPDATE ON cml_metadata  TO user1_role;
GRANT SELECT, INSERT, UPDATE ON cml_stats     TO user1_role;

-- webserver_role: read-only on base tables.
-- It switches to a user role (SET ROLE) for write operations triggered
-- via the web UI; those operations then use the user role's permissions.
GRANT SELECT ON cml_data      TO webserver_role;
GRANT SELECT ON cml_metadata  TO webserver_role;
GRANT SELECT ON cml_stats     TO webserver_role;

-- Continuous aggregate view.
-- RLS cannot be enforced on the aggregate directly (see header note).
-- Queries must always filter by user_id at the application layer.
GRANT SELECT ON cml_data_1h TO user1_role, webserver_role;

-- Parser uses update_cml_stats() to upsert per-CML statistics.
-- Grant execute so user1_role can call it without superuser privileges.
GRANT EXECUTE ON FUNCTION update_cml_stats(TEXT, TEXT) TO user1_role;

-- ---------------------------------------------------------------------------
-- Step 4: Enable Row-Level Security on base tables
-- ---------------------------------------------------------------------------

ALTER TABLE cml_data     ENABLE ROW LEVEL SECURITY;
ALTER TABLE cml_metadata ENABLE ROW LEVEL SECURITY;
ALTER TABLE cml_stats    ENABLE ROW LEVEL SECURITY;

-- ---------------------------------------------------------------------------
-- Step 5: RLS policies for user1_role
--
-- Each policy binds the role to rows where user_id = 'user1'.
-- USING:      applied on SELECT / UPDATE / DELETE (which rows are visible).
-- WITH CHECK: applied on INSERT / UPDATE (which rows can be written).
-- ---------------------------------------------------------------------------

CREATE POLICY user1_cml_data_policy ON cml_data
    FOR ALL TO user1_role
    USING     (user_id = 'user1')
    WITH CHECK (user_id = 'user1');

CREATE POLICY user1_cml_metadata_policy ON cml_metadata
    FOR ALL TO user1_role
    USING     (user_id = 'user1')
    WITH CHECK (user_id = 'user1');

CREATE POLICY user1_cml_stats_policy ON cml_stats
    FOR ALL TO user1_role
    USING     (user_id = 'user1')
    WITH CHECK (user_id = 'user1');

-- ---------------------------------------------------------------------------
-- Step 6: RLS policies for webserver_role
--
-- webserver_role has a permissive (USING true) read-all policy so it can
-- execute admin / aggregate queries without role-switching overhead.
-- For per-user scoped reads the webserver does SET ROLE user1_role, which
-- causes user1_role's policies above to take effect instead.
-- ---------------------------------------------------------------------------

CREATE POLICY webserver_cml_data_policy ON cml_data
    FOR SELECT TO webserver_role
    USING (true);

CREATE POLICY webserver_cml_metadata_policy ON cml_metadata
    FOR SELECT TO webserver_role
    USING (true);

CREATE POLICY webserver_cml_stats_policy ON cml_stats
    FOR SELECT TO webserver_role
    USING (true);
