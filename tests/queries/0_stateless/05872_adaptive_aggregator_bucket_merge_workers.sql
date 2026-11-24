-- The adaptive bucket merge takes as many sources as its work keeps busy: one per 262144 records that it drains or
-- adopts, or per 4 MiB of memory that it reads, never fewer than its producers and never more than `max_threads`.
-- States that own memory (`uniqExact`) take every thread; a wide fixed-size state (a `Nullable` `Decimal` sum) follows
-- the work rule. Each cell compares its result with the ordinary aggregation, and the number of merge sources comes from
-- the logs.

SET max_threads = 8, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192,
    compile_aggregate_expressions = 0, optimize_aggregation_in_order = 0,
    collect_hash_table_stats_during_aggregation = 0,
    group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000,
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0,
    adaptive_aggregator_freeze_threshold = 16384, adaptive_aggregator_freeze_threshold_bytes = 4194304,
    log_processors_profiles = 1;

-- 23000 keys: the table freezes with 16384 of them and stages the other 6616.
SELECT 'small', count(), sum(cityHash64(*))
FROM (SELECT toUInt64(number % 23000) AS k, sum(number) FROM numbers(30000) GROUP BY k)
SETTINGS enable_adaptive_aggregator = 0;
SELECT 'small', count(), sum(cityHash64(*))
FROM (SELECT toUInt64(number % 23000) AS k, sum(number) FROM numbers(30000) GROUP BY k)
SETTINGS enable_adaptive_aggregator = 1, log_comment = 'small';

SELECT 'two small streams', count(), sum(cityHash64(*))
FROM (SELECT k, sum(v) FROM (SELECT toUInt64(number % 23000) AS k, number AS v FROM numbers(30000)
    UNION ALL SELECT toUInt64(number % 23000), number FROM numbers(30000, 30000)) GROUP BY k)
SETTINGS enable_adaptive_aggregator = 0;
SELECT 'two small streams', count(), sum(cityHash64(*))
FROM (SELECT k, sum(v) FROM (SELECT toUInt64(number % 23000) AS k, number AS v FROM numbers(30000)
    UNION ALL SELECT toUInt64(number % 23000), number FROM numbers(30000, 30000)) GROUP BY k)
SETTINGS enable_adaptive_aggregator = 1, log_comment = 'two small streams';

SELECT 'many records', count(), sum(cityHash64(*))
FROM (SELECT number AS k FROM numbers(2500000) GROUP BY k)
SETTINGS enable_adaptive_aggregator = 0;
SELECT 'many records', count(), sum(cityHash64(*))
FROM (SELECT number AS k FROM numbers(2500000) GROUP BY k)
SETTINGS enable_adaptive_aggregator = 1, log_comment = 'many records';

SELECT 'heavy states', count(), sum(cityHash64(*))
FROM (SELECT toUInt64(number % 23000) AS k, uniqExact(number) FROM numbers(30000) GROUP BY k)
SETTINGS enable_adaptive_aggregator = 0;
SELECT 'heavy states', count(), sum(cityHash64(*))
FROM (SELECT toUInt64(number % 23000) AS k, uniqExact(number) FROM numbers(30000) GROUP BY k)
SETTINGS enable_adaptive_aggregator = 1, log_comment = 'heavy states';

SELECT 'wide fixed states', count(), sum(cityHash64(*))
FROM (SELECT toUInt64(number % 23000) AS k, sum(toNullable(toDecimal32(number % 1000, 2))) FROM numbers(30000) GROUP BY k)
SETTINGS enable_adaptive_aggregator = 0;
SELECT 'wide fixed states', count(), sum(cityHash64(*))
FROM (SELECT toUInt64(number % 23000) AS k, sum(toNullable(toDecimal32(number % 1000, 2))) FROM numbers(30000) GROUP BY k)
SETTINGS enable_adaptive_aggregator = 1, log_comment = 'wide fixed states';

-- About 40 MB of keys, 16 MB of them in the table: the memory asks for more than 8 sources, the table alone for fewer.
SELECT 'wide keys', count(), sum(cityHash64(*))
FROM (SELECT concat(toString(number), repeat('x', 1000)) AS s, count() FROM numbers(40000) GROUP BY s)
SETTINGS enable_adaptive_aggregator = 0;
SELECT 'wide keys', count(), sum(cityHash64(*))
FROM (SELECT concat(toString(number), repeat('x', 1000)) AS s, count() FROM numbers(40000) GROUP BY s)
SETTINGS enable_adaptive_aggregator = 1, log_comment = 'wide keys';

-- The staged records go to disk, and the merge still counts their bytes.
SELECT 'wide keys, spilled', count(), sum(cityHash64(*))
FROM (SELECT concat(toString(number), repeat('x', 1000)) AS s, count() FROM numbers(40000) GROUP BY s)
SETTINGS enable_adaptive_aggregator = 0;
SELECT 'wide keys, spilled', count(), sum(cityHash64(*))
FROM (SELECT concat(toString(number), repeat('x', 1000)) AS s, count() FROM numbers(40000) GROUP BY s)
SETTINGS enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = '24Mi', log_comment = 'wide keys, spilled';

SYSTEM FLUSH LOGS query_log, processors_profile_log;

SELECT log_comment, ProfileEvents['AdaptiveAggregationLocalFreezes'] > 0 AS froze, ProfileEvents['AdaptiveAggregationSpills'] > 0 AS spilled
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('small', 'two small streams', 'many records', 'heavy states', 'wide fixed states', 'wide keys',
        'wide keys, spilled')
ORDER BY log_comment;

SELECT log_comment, count() AS merge_sources
FROM system.processors_profile_log AS processors
INNER JOIN
(
    SELECT query_id, log_comment
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment IN ('small', 'two small streams', 'many records', 'heavy states', 'wide fixed states', 'wide keys',
            'wide keys, spilled')
) AS queries USING (query_id)
WHERE processors.event_date >= yesterday() AND processors.name = 'AdaptiveBucketMergingSource'
GROUP BY log_comment
ORDER BY log_comment;
