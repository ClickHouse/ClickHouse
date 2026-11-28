-- `group_by_each_block_no_merge` creates a hash table for every block. It must not preallocate the size of the
-- whole result recorded by an ordinary run of the same query, and it must not overwrite that size with the sizes of
-- its per-block tables. Checked through `AggregationPreallocatedElementsInHashTables`.

SET max_threads = 1;
SET enable_parallel_replicas = 0;
SET collect_hash_table_stats_during_aggregation = 1;
SET max_size_to_preallocate_for_aggregation = 1000000000000;
-- Spilling stops the collection, an engaged adaptive aggregator ignores the hint.
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET enable_adaptive_aggregator = 0;
SET optimize_aggregation_in_order = 0;
SET max_block_size = 1000;

DROP TABLE IF EXISTS t_05896;
CREATE TABLE t_05896 (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05896 SELECT number FROM numbers(600000);

SELECT k, count() FROM t_05896 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05896_1_cold';
SELECT k, count() FROM t_05896 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05896_2_warm';
SELECT k, count() FROM t_05896 GROUP BY k FORMAT Null SETTINGS group_by_each_block_no_merge = 1, log_comment = 'q05896_3_each_block';
SELECT k, count() FROM t_05896 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05896_4_after_each_block';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['AggregationPreallocatedElementsInHashTables']
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
    AND log_comment LIKE 'q05896\_%'
ORDER BY log_comment;

DROP TABLE t_05896;
