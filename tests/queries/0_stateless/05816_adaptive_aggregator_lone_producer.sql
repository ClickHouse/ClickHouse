-- An aggregation over one input stream has one producer for the adaptive aggregator, whatever `max_threads` is.
-- The cells check the two things that are special about it.
-- Its merge takes the threads of the query: the bucket merge gets a source per thread, and the number of
-- streams does not cap them. Two streams with four threads get four sources as well.
-- Its frozen table thaws sooner than the tables of several producers, because nothing merges the table of a lone
-- producer on the ordinary path. A producer that sees its keys 8 times thaws when it is alone, and stays frozen
-- as one of two.
--
-- Every cell returns values that follow from its data, and its counters come from the logs.

SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192,
    compile_aggregate_expressions = 0, optimize_aggregation_in_order = 0,
    collect_hash_table_stats_during_aggregation = 0,
    group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000,
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0,
    adaptive_aggregator_freeze_threshold = 16384, adaptive_aggregator_freeze_threshold_bytes = 4194304,
    adaptive_aggregator_disable_thaw = 0, enable_adaptive_aggregator = 1, log_processors_profiles = 1;

SELECT 'one stream, unique keys', count(), sum(k), sum(c)
FROM (SELECT toUInt64(number) AS k, count() AS c FROM numbers(100000) GROUP BY k)
SETTINGS log_comment = 'one stream, unique keys';

-- The table freezes with keys 0 to 16383, and every pass over the 40000 keys stages the other 23616.
SELECT 'one stream, repeated keys', count(), sum(k), sum(c)
FROM (SELECT toUInt64(number % 40000) AS k, count() AS c FROM numbers(320000) GROUP BY k)
SETTINGS log_comment = 'one stream, repeated keys';

SELECT 'two streams, repeated keys', count(), sum(k), sum(c)
FROM
(
    SELECT toUInt64(number % 40000) AS k, count() AS c
    FROM (SELECT number FROM numbers(0, 320000) UNION ALL SELECT number FROM numbers(320000, 320000))
    GROUP BY k
)
SETTINGS log_comment = 'two streams, repeated keys';

SYSTEM FLUSH LOGS query_log, processors_profile_log;

SELECT log_comment, ProfileEvents['AdaptiveAggregationLocalFreezes'] AS frozen, ProfileEvents['AdaptiveAggregationThaws'] AS thawed
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('one stream, unique keys', 'one stream, repeated keys', 'two streams, repeated keys')
ORDER BY log_comment;

SELECT log_comment, count() AS merge_sources
FROM system.processors_profile_log AS processors
INNER JOIN
(
    SELECT query_id, log_comment
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment IN ('one stream, unique keys', 'one stream, repeated keys', 'two streams, repeated keys')
) AS queries USING (query_id)
WHERE processors.event_date >= yesterday() AND processors.name = 'AdaptiveBucketMergingSource'
GROUP BY log_comment
ORDER BY log_comment;
