-- The INTERVAL kinds of window frame offsets survive query plan serialization: a remote query with a serialized
-- plan gives the same results as a local one. If a kind were lost, the counts would be applied as days.

DROP TABLE IF EXISTS test_window_interval_offset_serialized;
CREATE TABLE test_window_interval_offset_serialized (d Date, val UInt32) ENGINE = MergeTree ORDER BY d;
INSERT INTO test_window_interval_offset_serialized VALUES
    ('2024-01-10', 1), ('2024-01-25', 2), ('2024-02-05', 3), ('2024-02-07', 4), ('2024-02-28', 5), ('2024-03-01', 6);

SELECT 'local';
SELECT d, val, sum(val) OVER (ORDER BY d RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND INTERVAL 2 DAY FOLLOWING) AS rolling
FROM test_window_interval_offset_serialized
ORDER BY d;

SELECT 'remote, serialized query plan';
SELECT d, val, sum(val) OVER (ORDER BY d RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND INTERVAL 2 DAY FOLLOWING) AS rolling
FROM remote('127.0.0.2', currentDatabase(), test_window_interval_offset_serialized)
ORDER BY d
SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, enable_parallel_replicas = 0;

DROP TABLE test_window_interval_offset_serialized;
