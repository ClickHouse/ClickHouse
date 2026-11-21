-- A constant GROUP BY key of 1 MiB or more is not folded by the analyzer, but a local aggregation still drops it.

-- Parallel replicas keep the key (checked at the end), as GROUPING SETS do under group_by_use_nulls.
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, group_by_use_nulls = 0;

SELECT countIf(explain ILIKE '%Keys:%'), countIf(explain ILIKE '%Keys:%xxxxxxxxxx%')
FROM (EXPLAIN actions = 1 SELECT number % 3 AS g, count() FROM numbers(10) GROUP BY repeat('xx', 1000000), g);
SELECT countIf(explain ILIKE '%Keys:%'), countIf(explain ILIKE '%Keys:%xxxxxxxxxx%')
FROM (EXPLAIN actions = 1 SELECT number % 2 AS g, count() FROM numbers(4) GROUP BY GROUPING SETS ((repeat('xx', 1000000), g), (g)));

-- The shape from the stress test: a LowCardinality constant, serialized per row while it was a key.
SELECT g, count() FROM (SELECT number % 13 AS g FROM numbers(1000))
GROUP BY repeat(toLowCardinality('\0\0\0\0\0\0\0\0\0\0a'), 1000000), g ORDER BY g;
SELECT countIf(explain ILIKE '%Keys:%'), countIf(explain ILIKE '%Keys:%xxxxxxxxxx%' OR explain ILIKE '%Keys:%repeat(%')
FROM (EXPLAIN actions = 1 SELECT g, count() FROM (SELECT number % 13 AS g FROM numbers(1000))
    GROUP BY repeat(toLowCardinality('xx'), 1000000), g);

SELECT length(k), g, count() FROM numbers(10) GROUP BY repeat('xx', 1000000) AS k, number % 3 AS g ORDER BY g;
SELECT length(k), g, count() FROM numbers(10) GROUP BY repeat('xx', 1000000) AS k, number % 3 AS g WITH TOTALS ORDER BY g;
SELECT length(k), g, count() FROM numbers(4) GROUP BY GROUPING SETS ((repeat('xx', 1000000) AS k, number % 2 AS g), (g)) ORDER BY ALL;
SELECT length(k), g, count() FROM numbers(4) GROUP BY GROUPING SETS ((repeat('xx', 1000000) AS k, number % 2 AS g), (g)) ORDER BY ALL SETTINGS group_by_use_nulls = 1;
SELECT length(k), g, count() FROM numbers(4) GROUP BY ROLLUP(repeat('xx', 1000000) AS k, number % 2 AS g) ORDER BY ALL;
SELECT g FROM numbers(4) GROUP BY repeat('xx', 1000000), number % 2 AS g ORDER BY g;
-- Without aggregate functions the key is removed while another key stays.
SELECT countIf(explain ILIKE '%Keys:%xxxxxxxxxx%') FROM (EXPLAIN actions = 1
    SELECT count() FROM (SELECT number % 3 AS g, count() FROM numbers(10) GROUP BY repeat('xx', 1000000), g));
SELECT count() FROM (SELECT number % 3 AS g, count() FROM numbers(10) GROUP BY repeat('xx', 1000000), g);
SELECT countIf(explain ILIKE '%Keys:%xxxxxxxxxx%') FROM (EXPLAIN actions = 1 SELECT 1 FROM numbers(3) GROUP BY repeat('xx', 1000000));
SELECT 1 FROM numbers(3) GROUP BY repeat('xx', 1000000);
SELECT count() FROM numbers(10) WHERE number > 100 GROUP BY repeat('xx', 1000000);
-- The same as for a smaller constant.
SELECT count() FROM numbers(10) WHERE number > 100 GROUP BY repeat('xx', 1000000) SETTINGS empty_result_for_aggregation_by_constant_keys_on_empty_set = 0;

-- When other servers run a part of the aggregation, the key stays a key. Without a local plan it is shown by name.
DROP TABLE IF EXISTS t_large_constant_key;
CREATE TABLE t_large_constant_key (number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO t_large_constant_key SELECT number FROM numbers(10);
SELECT countIf(explain ILIKE '%Keys:%xxxxxxxxxx%' OR explain ILIKE '%Keys:%repeat(%') > 0 FROM (EXPLAIN actions = 1
    SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_large_constant_key) GROUP BY repeat('xx', 1000000), number % 3);
SELECT length(k), g, count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_large_constant_key)
GROUP BY repeat('xx', 1000000) AS k, number % 3 AS g ORDER BY g;
SET automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3,
    parallel_replicas_min_number_of_rows_per_replica = 0;
SELECT countIf(explain ILIKE '%Keys:%xxxxxxxxxx%' OR explain ILIKE '%Keys:%repeat(%') > 0 FROM (EXPLAIN actions = 1
    SELECT count() FROM t_large_constant_key GROUP BY repeat('xx', 1000000), number % 3);
SELECT g, count() FROM t_large_constant_key GROUP BY repeat('xx', 1000000), number % 3 AS g ORDER BY g SETTINGS parallel_replicas_local_plan = 0;
SELECT g, count() FROM t_large_constant_key GROUP BY repeat('xx', 1000000), number % 3 AS g ORDER BY g SETTINGS parallel_replicas_local_plan = 1;
DROP TABLE t_large_constant_key;
