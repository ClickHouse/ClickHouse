-- The format of the plan stored in `system.query_log.query_plan` is still changing, so these settings are experimental.
SELECT name, tier FROM system.settings
WHERE name IN ('log_query_plans', 'log_query_plans_max_size')
ORDER BY name;
