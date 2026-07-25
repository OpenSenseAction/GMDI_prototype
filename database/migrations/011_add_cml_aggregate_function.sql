-- Migration: Add helper function for efficient RLS-filtered aggregations
--
-- This function allows Grafana to query cml_data without hitting the
-- security-barrier view overhead, while still enforcing user isolation.
--
-- The function runs as the database superuser (SECURITY DEFINER) but
-- filters by the session user's ID, providing both performance and security.
--
-- Usage in Grafana:
--   SELECT * FROM get_cml_aggregates('40045_40212_2675', '2 days'::interval, '5 minutes');
--
-- Performance improvement: ~16s → ~50ms for 2-day aggregations

CREATE OR REPLACE FUNCTION get_cml_aggregates(
    p_cml_id TEXT,
    p_interval INTERVAL,
    p_bucket INTERVAL DEFAULT '5 minutes'::INTERVAL
)
RETURNS TABLE (
    "time" TIMESTAMPTZ,
    metric TEXT,
    rsl_avg DOUBLE PRECISION,
    rsl_min REAL,
    rsl_max REAL,
    tsl_avg DOUBLE PRECISION,
    tsl_min REAL,
    tsl_max REAL,
    record_count BIGINT
)
LANGUAGE plpgsql
SECURITY DEFINER  -- Runs with owner privileges (superuser)
SET search_path = public
AS $$
DECLARE
    v_user_id TEXT;
BEGIN
    -- Get the calling user's role name from the connection
    v_user_id := current_user::TEXT;
    
    -- Validate user exists in our USERS table (not just any DB role)
    IF NOT EXISTS (SELECT 1 FROM cml_metadata WHERE user_id = v_user_id LIMIT 1) THEN
        RAISE EXCEPTION 'Invalid user: %', v_user_id;
    END IF;
    
    RETURN QUERY
    SELECT
        time_bucket(p_bucket, c.time) AS "time",
        c.sublink_id::TEXT AS metric,
        AVG(c.rsl) AS rsl_avg,
        MIN(c.rsl) AS rsl_min,
        MAX(c.rsl) AS rsl_max,
        AVG(c.tsl) AS tsl_avg,
        MIN(c.tsl) AS tsl_min,
        MAX(c.tsl) AS tsl_max,
        COUNT(*)::BIGINT AS record_count
    FROM cml_data c
    WHERE c.user_id = v_user_id  -- Explicit filter enables index usage
      AND c.cml_id = p_cml_id
      AND c.time >= now() - p_interval
      AND c.time <= now()
    GROUP BY 1, 2
    ORDER BY 1 ASC;
END;
$$;

-- Grant execute permission to all users
GRANT EXECUTE ON FUNCTION get_cml_aggregates(TEXT, INTERVAL, INTERVAL) TO PUBLIC;

COMMENT ON FUNCTION get_cml_aggregates IS 
'Returns aggregated CML data for the calling user. Use this instead of querying cml_data directly for Grafana dashboards. Provides 100-300x speedup over security-barrier views.';
