-- Reset explicit settings so this also works with randomized session settings.
SET compatibility = '';
SET use_query_condition_cache_for_time_conditions = DEFAULT;

-- Keep the setting off by default, including under older compatibility versions.
SELECT 'default', value
FROM system.settings WHERE name = 'use_query_condition_cache_for_time_conditions';
SELECT '26.9', value
FROM system.settings WHERE name = 'use_query_condition_cache_for_time_conditions'
SETTINGS compatibility = '26.9';
SELECT '26.10', value
FROM system.settings WHERE name = 'use_query_condition_cache_for_time_conditions'
SETTINGS compatibility = '26.10';
SELECT '26.11', value
FROM system.settings WHERE name = 'use_query_condition_cache_for_time_conditions'
SETTINGS compatibility = '26.11';

SELECT version, change.previous_value, change.new_value
FROM system.settings_changes
ARRAY JOIN changes AS change
WHERE type = 'Session' AND change.name = 'use_query_condition_cache_for_time_conditions'
ORDER BY version;
