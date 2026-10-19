-- Random settings limits: index_granularity=(8192, None); index_granularity_bytes=(10485760, None)
-- An aggregation that caps its groups (the trivial GROUP BY LIMIT cutoff, or `max_rows_to_group_by` in
-- `any`/`break` mode), or aggregates over a capped one, keeps its own hash table statistics per cap value;
-- the next uncapped run still preallocates. A `throw` cap keeps sharing the uncapped entry. Checked through
-- `AggregationPreallocatedElementsInHashTables`; 6e5 groups exceed the 5e5 preallocation bound.

SET max_threads = 1;
SET enable_parallel_replicas = 0;
SET collect_hash_table_stats_during_aggregation = 1;
SET max_size_to_preallocate_for_aggregation = 1000000000000;
-- Spilling stops the collection, an engaged adaptive aggregator ignores the hint, the top-K heap replaces the cutoff.
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET enable_adaptive_aggregator = 0;
SET enable_group_by_top_k_optimization = 0;
SET optimize_trivial_group_by_limit_query = 1;

DROP TABLE IF EXISTS t_05406;
CREATE TABLE t_05406 (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05406 SELECT number FROM numbers(600000);

-- The planner cutoff, for a projection with aggregate functions.
SELECT k, count() FROM t_05406 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05406_1a_agg_cold';
SELECT k, count() FROM t_05406 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05406_1b_agg_warm';
SELECT k, count() FROM t_05406 GROUP BY k LIMIT 10 FORMAT Null SETTINGS log_comment = 'q05406_1c_agg_limit';
SELECT k, count() FROM t_05406 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05406_1d_agg_after_limit';
SELECT k, count() FROM t_05406 GROUP BY k LIMIT 550000 FORMAT Null SETTINGS log_comment = 'q05406_1e_agg_limit_550k';
SELECT k, count() FROM t_05406 GROUP BY k LIMIT 550000 FORMAT Null SETTINGS log_comment = 'q05406_1f_agg_limit_550k_warm';
SELECT k, count() FROM t_05406 GROUP BY k LIMIT 10 FORMAT Null SETTINGS log_comment = 'q05406_1g_agg_limit_10_again';

-- The settings pass, for a projection without aggregate functions.
SELECT k FROM t_05406 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05406_2a_keys_cold';
SELECT k FROM t_05406 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05406_2b_keys_warm';
SELECT k FROM t_05406 GROUP BY k LIMIT 10 FORMAT Null SETTINGS log_comment = 'q05406_2c_keys_limit';
SELECT k FROM t_05406 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05406_2d_keys_after_limit';

-- User caps on the aggregation of the first group.
SELECT k, count() FROM t_05406 GROUP BY k FORMAT Null SETTINGS max_rows_to_group_by = 20, group_by_overflow_mode = 'any', log_comment = 'q05406_3a_any';
SELECT k, count() FROM t_05406 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05406_3b_after_any';
SELECT k, count() FROM t_05406 GROUP BY k FORMAT Null SETTINGS max_rows_to_group_by = 30, group_by_overflow_mode = 'break', log_comment = 'q05406_3c_break';
SELECT k, count() FROM t_05406 GROUP BY k FORMAT Null SETTINGS log_comment = 'q05406_3d_after_break';
SELECT k, count() FROM t_05406 GROUP BY k FORMAT Null SETTINGS max_rows_to_group_by = 10000000, log_comment = 'q05406_4_throw';

-- An outer aggregation over an inner one capped by the subquery's own settings.
SELECT s, count() FROM (SELECT k, sum(k) AS s FROM t_05406 GROUP BY k) GROUP BY s FORMAT Null SETTINGS log_comment = 'q05406_5a_nested_cold';
SELECT s, count() FROM (SELECT k, sum(k) AS s FROM t_05406 GROUP BY k) GROUP BY s FORMAT Null SETTINGS log_comment = 'q05406_5b_nested_warm';
SELECT s, count() FROM (SELECT k, sum(k) AS s FROM t_05406 GROUP BY k SETTINGS max_rows_to_group_by = 40, group_by_overflow_mode = 'any') GROUP BY s FORMAT Null SETTINGS log_comment = 'q05406_5c_nested_inner_capped';
SELECT s, count() FROM (SELECT k, sum(k) AS s FROM t_05406 GROUP BY k) GROUP BY s FORMAT Null SETTINGS log_comment = 'q05406_5d_nested_after';
-- A cap the data never reaches keeps every group, so the capped nested query reuses its own entries.
SELECT s, count() FROM (SELECT k, sum(k) AS s FROM t_05406 GROUP BY k SETTINGS max_rows_to_group_by = 2000000, group_by_overflow_mode = 'any') GROUP BY s FORMAT Null SETTINGS log_comment = 'q05406_5e_nested_cap_unreached';
SELECT s, count() FROM (SELECT k, sum(k) AS s FROM t_05406 GROUP BY k SETTINGS max_rows_to_group_by = 2000000, group_by_overflow_mode = 'any') GROUP BY s FORMAT Null SETTINGS log_comment = 'q05406_5f_nested_cap_unreached_warm';

-- The same cap from the settings and from the planner cutoff: the settings cap keeps the whole first block, the cutoff only 10 keys.
-- The entries of `numbers` are shared by every database, so the cold run is not checked.
SELECT number AS k, count() FROM numbers(600000) GROUP BY k FORMAT Null SETTINGS max_block_size = 600000, max_rows_to_group_by = 10, group_by_overflow_mode = 'any';
SELECT number AS k, count() FROM numbers(600000) GROUP BY k FORMAT Null SETTINGS max_block_size = 600000, max_rows_to_group_by = 10, group_by_overflow_mode = 'any', log_comment = 'q05406_6a_any_block_warm';
SELECT number AS k, count() FROM numbers(600000) GROUP BY k LIMIT 10 FORMAT Null SETTINGS max_block_size = 600000, log_comment = 'q05406_6b_limit_same_cap';
SELECT number AS k, count() FROM numbers(600000) GROUP BY k FORMAT Null SETTINGS max_block_size = 600000, max_rows_to_group_by = 10, group_by_overflow_mode = 'any', log_comment = 'q05406_6c_any_block_after_limit';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['AggregationPreallocatedElementsInHashTables']
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
    AND log_comment LIKE 'q05406\_%'
ORDER BY log_comment;

DROP TABLE t_05406;
