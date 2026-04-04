-- Tags: no-parallel
SET max_execution_time=30;

-- This test exercises the bug where processWarning methods use stale metrics after values.swap(new_values)
-- The bug is in AsynchronousMetrics.cpp:2428-2431 where new_values contains OLD metrics after swap
-- but processWarningForMutationStats/Memory/CPU are called with stale new_values instead of fresh values

-- Clean state
DROP TABLE IF EXISTS test_stale_metrics_99901;

-- Create a MergeTree table to generate mutations and trigger warnings
CREATE TABLE test_stale_metrics_99901 (
    id UInt64,
    data String,
    timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY id;

-- Insert some data to have parts to mutate
INSERT INTO test_stale_metrics_99901 SELECT number, 'data_' || toString(number), now() FROM numbers(1000);

-- Create multiple mutations to increase NumberOfPendingMutations metric
-- These will create entries that the processWarningForMutationStats function should detect
ALTER TABLE test_stale_metrics_99901 UPDATE data = 'updated_1_' || data WHERE id % 10 = 0;
ALTER TABLE test_stale_metrics_99901 UPDATE data = 'updated_2_' || data WHERE id % 10 = 1;
ALTER TABLE test_stale_metrics_99901 UPDATE data = 'updated_3_' || data WHERE id % 10 = 2;
ALTER TABLE test_stale_metrics_99901 UPDATE data = 'updated_4_' || data WHERE id % 10 = 3;
ALTER TABLE test_stale_metrics_99901 UPDATE data = 'updated_5_' || data WHERE id % 10 = 4;

-- Force metric updates multiple times to trigger the code path where the bug occurs
-- This exercises the AsynchronousMetrics.cpp:2424-2431 path repeatedly
SYSTEM RELOAD ASYNCHRONOUS METRICS;
SELECT sleep(0.1); -- Brief pause to let metrics settle
SYSTEM RELOAD ASYNCHRONOUS METRICS;
SELECT sleep(0.1);
SYSTEM RELOAD ASYNCHRONOUS METRICS;

-- Check if we can observe the NumberOfPendingMutations metric
-- The bug would cause processWarningForMutationStats to use stale values
SELECT
    COALESCE((SELECT value > 0 FROM system.asynchronous_metrics WHERE metric = 'NumberOfPendingMutations' LIMIT 1), 0) as has_pending_mutations,
    'NumberOfPendingMutations metric checked' as status;

-- Force one more metric reload to exercise the buggy code path again
SYSTEM RELOAD ASYNCHRONOUS METRICS;

-- Check if warnings are properly updated (this tests the processWarning functions)
-- The bug might cause warnings to be based on stale metrics
SELECT 
    count(*) as warning_count,
    'Warnings potentially affected by stale metrics' as note
FROM system.warnings 
WHERE message LIKE '%pending mutations%' OR message LIKE '%memory%' OR message LIKE '%CPU%';

-- Verify that mutations were created (they may complete quickly)
SELECT
    count(*) > 0 as mutations_exist,
    'Mutations created to test metric processing' as status
FROM system.mutations
WHERE table = 'test_stale_metrics_99901';

-- Final metric reload to ensure we exercise the buggy swap logic one more time
SYSTEM RELOAD ASYNCHRONOUS METRICS;

-- This test primarily exercises the code path where the bug exists.
-- Even if the output looks correct, sanitizers in CI (TSAN, ASAN) may detect:
-- - Race conditions when accessing metrics after swap
-- - Use of stale data in processWarning methods
-- - Memory consistency issues in concurrent access

DROP TABLE IF EXISTS test_stale_metrics_99901;
