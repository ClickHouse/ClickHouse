SET max_threads_min_free_memory_per_thread = 0;
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 50000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET optimize_aggregation_in_order = 0;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_rows_to_group_by = 0;
SET enable_parallel_single_level_merge = 1;
SET enable_parallel_single_level_merge_by_index = 1;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 16384;

-- The tables of producers that never freeze merge by their cell indexes, also when there are fewer tables than
-- threads: one input stream from `numbers` leaves one table, and two lanes leave two. The keys stay far below the
-- freeze threshold. Each query compares its result with the one of the ordinary aggregation.

-- A lone table needs no merge, but `max_block_size = 100` splits its 2000 groups into many chunks, which one worker
-- converts by the partitions of the index.
SELECT
    (SELECT sum(cityHash64(k, c)) FROM (SELECT toUInt64(number % 2000) AS k, count() AS c FROM numbers(20000) GROUP BY k))
  = (SELECT sum(cityHash64(k, c)) FROM (SELECT toUInt64(number % 2000) AS k, count() AS c FROM numbers(20000) GROUP BY k) SETTINGS enable_adaptive_aggregator = 0)
SETTINGS max_threads = 1, max_block_size = 100, log_comment = 'lone_chunks';

-- The same table fits one chunk of `max_block_size = 65536` rows, so the serial path converts it.
SELECT
    (SELECT sum(cityHash64(k, c)) FROM (SELECT toUInt64(number % 2000) AS k, count() AS c FROM numbers(20000) GROUP BY k))
  = (SELECT sum(cityHash64(k, c)) FROM (SELECT toUInt64(number % 2000) AS k, count() AS c FROM numbers(20000) GROUP BY k) SETTINGS enable_adaptive_aggregator = 0)
SETTINGS max_threads = 1, max_block_size = 65536, log_comment = 'lone_one_chunk';

-- The states of `uniqExact` are heavy, so the merge threads of the step, 4 of them, convert the lone table.
SELECT
    (SELECT sum(cityHash64(k, u)) FROM (SELECT toUInt64(number % 1000) AS k, uniqExact(number) AS u FROM numbers(20000) GROUP BY k))
  = (SELECT sum(cityHash64(k, u)) FROM (SELECT toUInt64(number % 1000) AS k, uniqExact(number) AS u FROM numbers(20000) GROUP BY k) SETTINGS enable_adaptive_aggregator = 0)
SETTINGS max_threads = 4, max_block_size = 65536, log_comment = 'lone_workers';

-- Each lane feeds one producer, and both lanes hold the same 1000 keys. The two tables merge on the 4 merge threads of
-- the step, more than one per table.
CREATE VIEW two_lanes AS
    SELECT number FROM numbers(0, 10000)
    UNION ALL SELECT number FROM numbers(10000, 10000);

SELECT
    (SELECT sum(cityHash64(k, u)) FROM (SELECT toUInt64(number % 1000) AS k, uniqExact(number) AS u FROM two_lanes GROUP BY k))
  = (SELECT sum(cityHash64(k, u)) FROM (SELECT toUInt64(number % 1000) AS k, uniqExact(number) AS u FROM two_lanes GROUP BY k) SETTINGS enable_adaptive_aggregator = 0)
SETTINGS max_threads = 4, max_block_size = 65536, log_comment = 'few_tables';

-- Every partition of the indexes merges once: 256 for one producer, and 512 for two, whose layout splits each bucket in
-- two. The serial path merges none.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['AggregationPartitionsMergedByCellIndex']
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND log_comment IN ('lone_chunks', 'lone_one_chunk', 'lone_workers', 'few_tables')
ORDER BY log_comment;
