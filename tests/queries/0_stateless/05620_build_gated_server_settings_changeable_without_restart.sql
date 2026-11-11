-- These settings are changeable without a restart whether or not their component is built in.
SELECT name, changeable_without_restart
FROM system.server_settings
WHERE name IN ('distributed_cache_write_pool_size', 'gpu_expression_kernel_cache_elements_size', 'gpu_expression_kernel_cache_size')
ORDER BY name;

-- A setting without a live value reports the same value as `getServerSetting`.
SELECT toUInt64(value) = toUInt64(getServerSetting('gpu_expression_kernel_cache_size'))
FROM system.server_settings
WHERE name = 'gpu_expression_kernel_cache_size';
