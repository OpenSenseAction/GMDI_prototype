-- 008_add_file_processing_log.sql
--
-- Adds a lightweight audit table that the parser writes to on every file
-- outcome (archived = success, quarantined = failure).  Used by the Grafana
-- "Pipeline Health" dashboard to track per-user pipeline activity.

CREATE TABLE IF NOT EXISTS file_processing_log (
    id            BIGSERIAL PRIMARY KEY,
    user_id       TEXT        NOT NULL,
    filename      TEXT        NOT NULL,
    status        TEXT        NOT NULL CHECK (status IN ('archived', 'quarantined')),
    rows_written  INT,
    error_message TEXT,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Fast lookups used by the dashboard: per-user timeline and quarantine list.
CREATE INDEX IF NOT EXISTS file_processing_log_user_time_idx
    ON file_processing_log (user_id, processed_at DESC);

CREATE INDEX IF NOT EXISTS file_processing_log_status_time_idx
    ON file_processing_log (status, processed_at DESC);

-- Row-Level Security: each login role only sees its own rows (user_id = current_user).
-- myuser (superuser) bypasses RLS so the parser continues to INSERT without restriction.
ALTER TABLE file_processing_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE file_processing_log FORCE ROW LEVEL SECURITY;
CREATE POLICY user_isolation ON file_processing_log
    USING (user_id = current_user);

-- Grant read access to all user roles so Grafana dashboards can query this table.
-- Each role connects to Postgres as their own login (demo_openmrg, demo_orange_cameroun)
-- and Grafana reads the log for that org's datasource.
-- webserver_role needs it for any admin views.
GRANT SELECT ON file_processing_log TO demo_openmrg, demo_orange_cameroun, webserver_role;

-- Sequence used by the BIGSERIAL primary key: needed for INSERTs by the parser
-- (which connects as myuser/superuser, so this is a no-op in practice,
-- but keeps the intent explicit for future role changes).
GRANT USAGE ON SEQUENCE file_processing_log_id_seq TO demo_openmrg, demo_orange_cameroun;
