-- Tags: no-parallel-replicas: index analysis runs on the replicas, the initiator does not see it

SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET optimize_trivial_count_query = 0;
SET optimize_use_projections = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET distributed_index_analysis = 0;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
-- a throwing `max_rows_to_read` turns off skip indexes on data read, the test profile sets it
SET max_rows_to_read = 0;

DROP TABLE IF EXISTS t_skip_index_early_exit;
DROP TABLE IF EXISTS t_skip_index_early_exit_dim;

CREATE TABLE t_skip_index_early_exit
(
    k UInt64,
    a UInt64,
    b UInt64,
    INDEX idx_a a TYPE minmax GRANULARITY 1,
    INDEX idx_b b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE t_skip_index_early_exit_dim (b UInt64) ENGINE = MergeTree ORDER BY b;

-- one part of eight granules
INSERT INTO t_skip_index_early_exit SELECT number, number, number FROM numbers(64);
INSERT INTO t_skip_index_early_exit_dim VALUES (5);

-- idx_a excludes every granule, so idx_b is not evaluated
SELECT k FROM t_skip_index_early_exit WHERE a = 500 AND b = 500 SETTINGS use_skip_indexes_on_data_read = 1, log_comment = 'skip_index_early_exit_1';
SELECT k FROM t_skip_index_early_exit WHERE a = 500 AND b = 500 SETTINGS use_skip_indexes_on_data_read = 0, log_comment = 'skip_index_early_exit_2';
-- idx_a keeps a granule, so both indexes are evaluated
SELECT k FROM t_skip_index_early_exit WHERE a = 5 AND b = 5 SETTINGS use_skip_indexes_on_data_read = 1, log_comment = 'skip_index_early_exit_3';
-- idx_a excludes every granule, so the join runtime filter does not evaluate idx_b; PREWHERE keeps idx_a at read time
SELECT count() FROM (SELECT b FROM t_skip_index_early_exit PREWHERE a = 500) AS t INNER JOIN t_skip_index_early_exit_dim AS d ON t.b = d.b
SETTINGS use_skip_indexes_on_data_read = 1, log_comment = 'skip_index_early_exit_4';
SELECT count() FROM (SELECT b FROM t_skip_index_early_exit PREWHERE a = 5) AS t INNER JOIN t_skip_index_early_exit_dim AS d ON t.b = d.b
SETTINGS use_skip_indexes_on_data_read = 1, log_comment = 'skip_index_early_exit_5';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), used_data_skipping_indices)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'skip_index_early_exit_%'
ORDER BY log_comment;

DROP TABLE t_skip_index_early_exit;
DROP TABLE t_skip_index_early_exit_dim;
