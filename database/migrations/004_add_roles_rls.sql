-- Migration 004: Create database roles and enable Row-Level Security
--
-- Part of PR feat/db-roles-rls (multi-user RLS support, Phase 2).
-- Run this AFTER migrations 001–003 (feat/db-add-user-id).
--
-- Role naming convention:
--   PostgreSQL login role names intentionally match the user_id values
--   stored in the data tables (e.g. role "user1" ↔ user_id = 'user1').
--   This allows RLS policies to use current_user for automatic filtering,
--   and lets cml_data_1h_secure (a security-barrier view over the
--   continuous aggregate) enforce per-user isolation at the DB level —
--   eliminating the need for application-level WHERE user_id = ? clauses
--   on the aggregate.
--
-- What this migration does:
--   1. Creates user1 login role (parser + webserver for user1's data).
--   2. Creates webserver_role (read-all for admin queries;
--      SET ROLE user1 for DB-enforced scoped reads).
--   3. Grants table/function permissions to each role.
--   4. Enables Row-Level Security on cml_data, cml_metadata, cml_stats.
--   5. Creates a single generic current_user policy per base table
--      (works for all users; no per-user policy needed at onboarding).
--   6. Creates cml_data_1h_secure — a security_barrier view over the
--      continuous aggregate with WHERE user_id = current_user.
--      User roles get SELECT only on this view (not the raw aggregate).
--      webserver_role retains direct SELECT on cml_data_1h for admin
--      queries, and also on cml_data_1h_secure when it SETROLEs.
--
-- Backward-compatibility:
--   myuser (superuser) bypasses RLS by default, so the existing parser
--   and webserver — which both still connect as myuser — continue to work
--   without any changes until PR3 (feat/parser-user-id) and
--   PR5 (feat/webserver-auth) wire up the new role credentials.
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
-- Step 1: Create roles (idempotent via DO blocks)
--
-- Role "user1" matches user_id = 'user1' in the data, enabling the
-- current_user-based RLS policies and security-barrier view below.
-- ---------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'user1') THEN
        CREATE ROLE user1 LOGIN PASSWORD 'user1password';
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

-- Allow webserver_role to impersonate user roles (SET ROLE user1).
-- After SET ROLE user1 the session current_user becomes 'user1', so the
-- generic RLS policies and cml_data_1h_secure both filter automatically.
GRANT user1 TO webserver_role;

-- ---------------------------------------------------------------------------
-- Step 2: Schema access
-- ---------------------------------------------------------------------------

GRANT USAGE ON SCHEMA public TO user1, webserver_role;

-- ---------------------------------------------------------------------------
-- Step 3: Table permissions
-- ---------------------------------------------------------------------------

-- user1: INSERT/SELECT/UPDATE on the three data tables.
-- No DELETE: raw data is never deleted by design.
GRANT SELECT, INSERT, UPDATE ON cml_data     TO user1;
GRANT SELECT, INSERT, UPDATE ON cml_metadata TO user1;
GRANT SELECT, INSERT, UPDATE ON cml_stats    TO user1;

-- webserver_role: read-only on base tables.
-- Per-user scoped reads are done via SET ROLE user1; the user role's
-- permissions and RLS policies then take effect automatically.
GRANT SELECT ON cml_data     TO webserver_role;
GRANT SELECT ON cml_metadata TO webserver_role;
GRANT SELECT ON cml_stats    TO webserver_role;

-- Parser calls update_cml_stats() to upsert per-CML statistics.
GRANT EXECUTE ON FUNCTION update_cml_stats(TEXT, TEXT) TO user1;

-- ---------------------------------------------------------------------------
-- Step 4: Enable Row-Level Security on base tables
-- ---------------------------------------------------------------------------

ALTER TABLE cml_data     ENABLE ROW LEVEL SECURITY;
ALTER TABLE cml_metadata ENABLE ROW LEVEL SECURITY;
ALTER TABLE cml_stats    ENABLE ROW LEVEL SECURITY;

-- ---------------------------------------------------------------------------
-- Step 5: Generic current_user RLS policies for user roles
--
-- Because role name = user_id value, a single policy per table covers
-- every user — no per-user policy is needed when onboarding new users.
--
-- USING:      applied on SELECT / UPDATE / DELETE (visible rows).
-- WITH CHECK: applied on INSERT / UPDATE (writable rows).
--
-- webserver_role has a separate permissive (USING true) read-all policy
-- so it can run admin / cross-user aggregate queries without SET ROLE.
-- When it does SET ROLE user1, the session role becomes user1 and this
-- generic policy takes over instead.
-- ---------------------------------------------------------------------------

CREATE POLICY user_cml_data_policy ON cml_data
    FOR ALL
    USING     (user_id = current_user)
    WITH CHECK (user_id = current_user);

CREATE POLICY user_cml_metadata_policy ON cml_metadata
    FOR ALL
    USING     (user_id = current_user)
    WITH CHECK (user_id = current_user);

CREATE POLICY user_cml_stats_policy ON cml_stats
    FOR ALL
    USING     (user_id = current_user)
    WITH CHECK (user_id = current_user);

-- Permissive read-all policies for webserver_role (admin / cross-user use).
CREATE POLICY webserver_cml_data_policy ON cml_data
    FOR SELECT TO webserver_role
    USING (true);

CREATE POLICY webserver_cml_metadata_policy ON cml_metadata
    FOR SELECT TO webserver_role
    USING (true);

CREATE POLICY webserver_cml_stats_policy ON cml_stats
    FOR SELECT TO webserver_role
    USING (true);

-- ---------------------------------------------------------------------------
-- Step 6: Security-barrier view over cml_data_1h (continuous aggregate)
--
-- PostgreSQL cannot apply RLS to materialized views, so cml_data_1h itself
-- has no automatic row filtering.  cml_data_1h_secure wraps it with
-- WHERE user_id = current_user and the security_barrier option, which
-- prevents the planner from pushing attacker-controlled predicates above
-- the security filter.
--
-- Usage pattern:
--   User roles (e.g. user1) query cml_data_1h_secure — DB-enforced,
--   no WHERE clause needed in the application.
--
--   webserver_role queries cml_data_1h_secure after SET ROLE user1 for
--   user-scoped pages (fully DB-enforced).  For admin / cross-user
--   aggregate queries it queries cml_data_1h directly as webserver_role;
--   those queries must include WHERE user_id = ? at the application layer,
--   but that is acceptable for internal admin paths.
-- ---------------------------------------------------------------------------

CREATE VIEW cml_data_1h_secure WITH (security_barrier) AS
SELECT * FROM cml_data_1h
WHERE user_id = current_user;

-- User roles: access only the secure view, not the underlying aggregate.
GRANT SELECT ON cml_data_1h_secure TO user1;

-- webserver_role: both views (see usage pattern above).
GRANT SELECT ON cml_data_1h        TO webserver_role;
GRANT SELECT ON cml_data_1h_secure TO webserver_role;
