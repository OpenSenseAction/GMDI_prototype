-- Migration 011: Add cml_rain_stats view for rain rate statistics
-- This view provides aggregated statistics for rain rate data, similar to cml_stats

-- Create aggregated statistics view for rain rates
CREATE OR REPLACE VIEW cml_rain_stats AS
SELECT
    r.cml_id::text,
    r.user_id,
    COUNT(*)                                                              AS total_records,
    COUNT(CASE WHEN r.r IS NOT NULL AND r.r > 0 THEN 1 END)               AS valid_records,
    ROUND(
        100.0 * COUNT(CASE WHEN r.r IS NOT NULL AND r.r > 0 THEN 1 END) / COUNT(*),
        2
    )                                                                     AS completeness_percent,
    ROUND(AVG(r.r)::numeric, 2)                                           AS mean_rain_rate,
    ROUND(STDDEV(r.r)::numeric, 2)                                        AS stddev_rain_rate,
    MAX(r.r)                                                              AS max_rain_rate,
    -- Last rain rate (most recent non-zero value)
    (
        SELECT r2.r FROM cml_rain_data r2
        WHERE r2.cml_id = r.cml_id
          AND r2.user_id = r.user_id
          AND r2.r IS NOT NULL AND r2.r > 0
        ORDER BY r2.time DESC LIMIT 1
    )                                                                     AS last_rain_rate,
    -- 6-hour window statistics
    ROUND(
        100.0 * COUNT(CASE WHEN r.r IS NOT NULL AND r.r > 0 AND r.time >= NOW() - INTERVAL '6 hours' THEN 1 END)
              / NULLIF(COUNT(*) FILTER (WHERE r.time >= NOW() - INTERVAL '6 hours'), 0),
        2
    )                                                                     AS completeness_percent_6h,
    COUNT(*) FILTER (WHERE r.time >= NOW() - INTERVAL '6 hours')          AS total_records_6h,
    COUNT(CASE WHEN r.r IS NOT NULL AND r.r > 0 AND r.time >= NOW() - INTERVAL '6 hours' THEN 1 END) AS valid_records_6h,
    ROUND(AVG(r.r) FILTER (WHERE r.time >= NOW() - INTERVAL '6 hours')::numeric, 2) AS mean_rain_rate_6h,
    ROUND(STDDEV(r.r) FILTER (WHERE r.time >= NOW() - INTERVAL '6 hours')::numeric, 2) AS stddev_rain_rate_6h,
    -- 1-hour window statistics
    ROUND(
        100.0 * COUNT(CASE WHEN r.r IS NOT NULL AND r.r > 0 AND r.time >= NOW() - INTERVAL '1 hour' THEN 1 END)
              / NULLIF(COUNT(*) FILTER (WHERE r.time >= NOW() - INTERVAL '1 hour'), 0),
        2
    )                                                                     AS completeness_percent_1h,
    ROUND(AVG(r.r) FILTER (WHERE r.time >= NOW() - INTERVAL '1 hour')::numeric, 2) AS mean_rain_rate_1h,
    ROUND(STDDEV(r.r) FILTER (WHERE r.time >= NOW() - INTERVAL '1 hour')::numeric, 2) AS stddev_rain_rate_1h,
    NOW()                                                                 AS last_update
FROM cml_rain_data r
GROUP BY r.cml_id, r.user_id;

-- Grant permissions to webserver_role for admin access
GRANT SELECT ON cml_rain_stats TO webserver_role;
