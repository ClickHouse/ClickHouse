-- Test memory for skip indexes
ALTER TABLE test_mt ADD INDEX idx_name name TYPE bloom_filter GRANULARITY 1;
ALTER TABLE test_mt ADD INDEX idx_value value TYPE minmax GRANULARITY 1;
ALTER TABLE test_mt MATERIALIZE INDEX idx_name SETTINGS mutations_sync = 2;
ALTER TABLE test_mt MATERIALIZE INDEX idx_value SETTINGS mutations_sync = 2;

-- Exercise secondary-index marks loading.
SELECT count()
FROM test_mt
WHERE name = 'name_42'
SETTINGS force_data_skipping_indices = 'idx_name';
