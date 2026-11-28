-- These settings first shipped, or changed their default, in 24.11, and their history once also recorded the same change
-- under 24.10. Each has one record now, which leaves the values set by `compatibility` as they were.

SELECT c.name, arraySort(groupArray(version))
FROM system.settings_changes ARRAY JOIN changes AS c
WHERE type = 'Session' AND c.name IN (
    'azure_check_objects_after_upload',
    'backup_restore_failure_after_host_disconnected_for_seconds',
    'backup_restore_finish_timeout_after_error_sec',
    'backup_restore_keeper_max_retries',
    'backup_restore_keeper_max_retries_while_handling_error',
    'backup_restore_keeper_max_retries_while_initializing',
    'distributed_cache_discard_connection_if_unread_data',
    'parallel_replicas_local_plan')
GROUP BY c.name
ORDER BY c.name;

SET compatibility = '24.10';
SELECT name, value FROM system.settings
WHERE name IN ('azure_check_objects_after_upload', 'backup_restore_keeper_max_retries')
ORDER BY name;

SET compatibility = '24.9';
SELECT name, value FROM system.settings
WHERE name IN ('azure_check_objects_after_upload', 'backup_restore_keeper_max_retries')
ORDER BY name;
