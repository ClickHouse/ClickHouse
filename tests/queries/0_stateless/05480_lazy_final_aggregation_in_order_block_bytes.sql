-- The aggregation of lazy `FINAL` must respect `aggregation_in_order_max_block_bytes` when it runs in order:
-- with a limit of zero it finished a block and merged it after every key, which made the query very slow.

DROP TABLE IF EXISTS t_lazy_final_agg_in_order;
CREATE TABLE t_lazy_final_agg_in_order (key UInt64, v UInt64, ver UInt64)
ENGINE = ReplacingMergeTree(ver) ORDER BY key SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_lazy_final_agg_in_order;
INSERT INTO t_lazy_final_agg_in_order SELECT number, number, 1 FROM numbers(20000);
INSERT INTO t_lazy_final_agg_in_order SELECT number, number + 1, 2 FROM numbers(10000);

SELECT count(), sum(v) FROM t_lazy_final_agg_in_order FINAL WHERE v % 10 = 3
SETTINGS query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0, optimize_aggregation_in_order = 1,
    aggregation_in_order_max_block_bytes = 50000000, max_block_size = 65536, log_comment = '05480_lazy_final_aggregation_in_order_block_bytes';

SYSTEM FLUSH LOGS query_log;
-- Before the fix, the aggregation merged every one of the 20000 keys on its own, allocating arena chunks for each.
SELECT ProfileEvents['ArenaAllocChunks'] < 1000 FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05480_lazy_final_aggregation_in_order_block_bytes' AND type = 'QueryFinish' AND event_date >= yesterday();

DROP TABLE t_lazy_final_agg_in_order;
