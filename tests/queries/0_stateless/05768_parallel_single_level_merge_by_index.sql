SET max_threads = 4;
SET max_threads_min_free_memory_per_thread = 0;
SET max_block_size = 65536;
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 50000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET optimize_aggregation_in_order = 0;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_rows_to_group_by = 0;
SET enable_parallel_single_level_merge = 1;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 16384;

-- Each lane feeds one thread. Every lane holds the same 1000 keys, so every table holds 1000 cells, far below
-- the freeze threshold, and the tables merge partition by partition. The states of `uniqExact` are heavy, so
-- the merge runs in parallel.
CREATE VIEW lanes AS
    SELECT number FROM numbers(0, 100000)
    UNION ALL SELECT number FROM numbers(100000, 100000)
    UNION ALL SELECT number FROM numbers(200000, 100000)
    UNION ALL SELECT number FROM numbers(300000, 100000);

-- With the setting on, the merge reads only the cells of each partition through the indexes of the tables.
SELECT
    (SELECT sum(cityHash64(k, u)) FROM (SELECT toUInt64(number % 1000) AS k, uniqExact(number) AS u FROM lanes GROUP BY k))
  = (SELECT sum(cityHash64(k, u)) FROM (SELECT toUInt64(number % 1000) AS k, uniqExact(number) AS u FROM lanes GROUP BY k) SETTINGS enable_adaptive_aggregator = 0)
SETTINGS enable_parallel_single_level_merge_by_index = 1, log_comment = 'by_index';

-- With the setting off, the merge scans every cell of every table for each partition.
SELECT
    (SELECT sum(cityHash64(k, u)) FROM (SELECT toUInt64(number % 1000) AS k, uniqExact(number) AS u FROM lanes GROUP BY k))
  = (SELECT sum(cityHash64(k, u)) FROM (SELECT toUInt64(number % 1000) AS k, uniqExact(number) AS u FROM lanes GROUP BY k) SETTINGS enable_adaptive_aggregator = 0)
SETTINGS enable_parallel_single_level_merge_by_index = 0, log_comment = 'scan';

-- The tables are indexed in both queries, and only the first query reads them through the indexes.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['AggregationIndexedTables'] > 0, ProfileEvents['AggregationPartitionsMergedByCellIndex'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND log_comment IN ('by_index', 'scan')
ORDER BY log_comment;
