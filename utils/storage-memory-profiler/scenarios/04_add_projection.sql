-- Test memory for projections
ALTER TABLE test_mt ADD PROJECTION proj_by_name (
    SELECT * ORDER BY name
);
ALTER TABLE test_mt MATERIALIZE PROJECTION proj_by_name SETTINGS mutations_sync = 2;

-- Nothing merges or mutates test_mt after this point. Prevent background merge selection from crossing the heap-dump checkpoint.
SYSTEM STOP MERGES test_mt;
