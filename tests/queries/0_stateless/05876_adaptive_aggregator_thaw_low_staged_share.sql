-- A lone producer's frozen table thaws once its missing keys repeat, even when they are a small share of its rows.
-- The keys here only just exceed the freeze threshold: the table absorbs most rows in place and stages the others.
-- Several producers keep their tables frozen while the staged records are under a quarter of their rows.
-- Misses that do not repeat keep the table of a lone producer frozen too.
--
-- Every cell returns values that follow from its data, and its counters come from the logs.

SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192,
    compile_aggregate_expressions = 0, optimize_aggregation_in_order = 0,
    collect_hash_table_stats_during_aggregation = 0,
    group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000,
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0,
    adaptive_aggregator_freeze_threshold = 16384, adaptive_aggregator_freeze_threshold_bytes = 4194304,
    enable_adaptive_aggregator = 1, adaptive_aggregator_disable_thaw = 0;

-- The table freezes with 16384 of the 20000 keys, and every pass over them stages the other 3616.
SELECT 'one stream, keys above the threshold', count(), sum(k), sum(c)
FROM (SELECT toUInt64(number % 20000) AS k, count() AS c FROM numbers(800000) GROUP BY k)
SETTINGS max_threads = 1, log_comment = 'one stream, keys above the threshold';

SELECT 'one stream, top-K by count', sum(c)
FROM (SELECT toUInt64(number % 20000) AS k, count() AS c FROM numbers(800000) GROUP BY k ORDER BY c DESC LIMIT 10)
SETTINGS max_threads = 1, log_comment = 'one stream, top-K by count';

SELECT 'two streams, keys above the threshold', count(), sum(k), sum(c)
FROM
(
    SELECT toUInt64(number % 20000) AS k, count() AS c
    FROM (SELECT number FROM numbers(0, 1600000) UNION ALL SELECT number FROM numbers(1600000, 1600000))
    GROUP BY k
)
SETTINGS max_threads = 2, log_comment = 'two streams, keys above the threshold';

-- 14400 keys repeat (no residue divisible by 10 occurs), and every tenth row brings a key of its own.
SELECT 'one stream, misses that do not repeat', count(), sum(c)
FROM
(
    SELECT if(number % 10 = 0, toUInt64(number) + 1000000, toUInt64(number % 16000)) AS k, count() AS c
    FROM numbers(800000)
    GROUP BY k
)
SETTINGS max_threads = 1, log_comment = 'one stream, misses that do not repeat';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['AdaptiveAggregationLocalFreezes'] AS frozen, ProfileEvents['AdaptiveAggregationThaws'] AS thawed
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('one stream, keys above the threshold', 'one stream, top-K by count',
        'two streams, keys above the threshold', 'one stream, misses that do not repeat')
ORDER BY log_comment;
