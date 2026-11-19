-- The parallel merge by hash partitions cuts its result into chunks for the fan-out of its output, as the
-- serial merge does (see 05257_aggregation_single_level_result_fanout): a `Resize` hands out whole chunks, so
-- the steps after the aggregation get as many threads as the result has chunks. Four producers merge here,
-- and the output fans out to eight streams. A chunk per merge worker would hold about a quarter of the result
-- of 12000 keys and leave half of the streams idle. The fan-out cuts it into chunks of about an eighth, 1500
-- rows, and the bound of 2250 rows leaves room for the uneven sizes of the partitions. Both ways of reading
-- the tables are checked: through the cell indexes that the producers of the adaptive aggregator build, and
-- by scanning every table for every partition.

SET max_threads = 8;
SET max_threads_min_free_memory_per_thread = 0;
SET max_block_size = 65536;
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 50000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET optimize_aggregation_in_order = 0;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET enable_parallel_single_level_merge = 1;
SET max_rows_to_group_by = 0;

-- Each lane feeds one producer, so four producers merge whatever the scheduling. Every lane holds all keys.
CREATE VIEW lanes AS
    SELECT number FROM numbers(0, 200000)
    UNION ALL SELECT number FROM numbers(200000, 200000)
    UNION ALL SELECT number FROM numbers(400000, 200000)
    UNION ALL SELECT number FROM numbers(600000, 200000);

-- The freeze thresholds are out of reach, so no table freezes and the merge reads the cell indexes.
SELECT 'cell index';
SELECT max(bs) <= 2250, count(), sum(c)
FROM (SELECT blockSize() AS bs, c FROM (SELECT toUInt64(number % 12000) AS k, count() AS c FROM lanes GROUP BY k))
SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 1000000,
    adaptive_aggregator_freeze_threshold_bytes = 1000000000, log_comment = 'cell_index';

SELECT 'scan';
SELECT max(bs) <= 2250, count(), sum(c)
FROM (SELECT blockSize() AS bs, c FROM (SELECT toUInt64(number % 12000) AS k, count() AS c FROM lanes GROUP BY k))
SETTINGS enable_adaptive_aggregator = 0, log_comment = 'scan';

-- The producers of the adaptive aggregation indexed their tables, and none of them froze.
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['AggregationIndexedTables'] > 0, ProfileEvents['AdaptiveAggregationLocalFreezes'] = 0
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = 'cell_index';

DROP VIEW lanes;
