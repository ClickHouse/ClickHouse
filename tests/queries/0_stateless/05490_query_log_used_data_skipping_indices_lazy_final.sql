-- Tags: no-parallel-replicas
-- no-parallel-replicas: index analysis runs on the replicas, the initiator does not see it

-- lazy FINAL that replaces the read keeps the drops of the replaced analysis

SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET distributed_index_analysis = 0;
SET max_rows_to_read = 0;
SET optimize_move_to_prewhere = 0;
SET use_skip_indexes_on_data_read = 0;
SET use_skip_indexes_if_final = 1;
SET use_skip_indexes_if_final_exact_mode = 0;

DROP TABLE IF EXISTS t_lazy_final;
DROP TABLE IF EXISTS t_lazy_final_split;

CREATE TABLE t_lazy_final (k UInt64, a UInt64, INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = ReplacingMergeTree
PARTITION BY intDiv(k, 32)
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

CREATE TABLE t_lazy_final_split AS t_lazy_final;
-- a merge would remove the intersecting part
SYSTEM STOP MERGES t_lazy_final_split;

-- two partitions of four granules, no part intersects another
INSERT INTO t_lazy_final SELECT number, number FROM numbers(64);

-- plus a part of two granules that intersects the first one, so lazy FINAL splits the parts
INSERT INTO t_lazy_final_split SELECT number, number FROM numbers(64);
INSERT INTO t_lazy_final_split SELECT number, number FROM numbers(16);

SELECT k FROM t_lazy_final FINAL WHERE k = 5 SETTINGS query_plan_optimize_lazy_final = 0, log_comment = 'lazy_final_1_off' FORMAT Null;
SELECT k FROM t_lazy_final FINAL WHERE k = 5 SETTINGS query_plan_optimize_lazy_final = 1, log_comment = 'lazy_final_1_on' FORMAT Null;
SELECT k FROM t_lazy_final FINAL WHERE a = 5 SETTINGS query_plan_optimize_lazy_final = 0, log_comment = 'lazy_final_2_off' FORMAT Null;
SELECT k FROM t_lazy_final FINAL WHERE a = 5 SETTINGS query_plan_optimize_lazy_final = 1, log_comment = 'lazy_final_2_on' FORMAT Null;
SELECT k FROM t_lazy_final_split FINAL WHERE a IN (5, 40) SETTINGS query_plan_optimize_lazy_final = 0, log_comment = 'lazy_final_3_off' FORMAT Null;
SELECT k FROM t_lazy_final_split FINAL WHERE a IN (5, 40) SETTINGS query_plan_optimize_lazy_final = 1, log_comment = 'lazy_final_3_on' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['GranulesDroppedByPartitionKey'],
    ProfileEvents['GranulesDroppedByPrimaryKey'],
    ProfileEvents['GranulesDroppedBySkipIndexes'],
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), used_data_skipping_indices)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'lazy_final_%'
ORDER BY log_comment;

DROP TABLE t_lazy_final;
DROP TABLE t_lazy_final_split;
