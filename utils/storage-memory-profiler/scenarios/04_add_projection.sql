-- Test memory for projections
ALTER TABLE test_mt ADD PROJECTION proj_by_name (
    SELECT * ORDER BY name
);
ALTER TABLE test_mt MATERIALIZE PROJECTION proj_by_name SETTINGS mutations_sync = 2;
