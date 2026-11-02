-- Tags: no-parallel, no-fasttest
-- Tag no-parallel: the failpoint is server-wide.
-- Tag no-fasttest: tests that must run alone are kept out of the fast test.
-- An in-order read splits a part between streams, and a parallel replicas follower receives the initiator's ranges
-- per stream. Its read-time skip-index input for the part must be the union of them: with only one stream's ranges,
-- the rows the other streams read are dropped.

SET automatic_parallel_replicas_mode = 0;
SET use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
-- The CI profile's max_rows_to_read with read_overflow_mode = 'throw' turns the data-read path off.
SET max_rows_to_read = 0, max_rows_to_read_leaf = 0;
SET use_query_condition_cache = 0;
SET max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_local_plan = 1, parallel_replicas_index_analysis_only_on_coordinator = 1;
-- Small `merge_tree_min_*_for_concurrent_read` let the initiator split the part between the streams.
SET max_threads = 4, optimize_read_in_order = 1, merge_tree_min_rows_for_concurrent_read = 640, merge_tree_min_bytes_for_concurrent_read = 1;

DROP TABLE IF EXISTS t_pr_follower_skip_idx_in_order;

CREATE TABLE t_pr_follower_skip_idx_in_order (k UInt64, v UInt64, INDEX iv v TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;

-- One part of 1000 granules with a matching row in every tenth one.
INSERT INTO t_pr_follower_skip_idx_in_order SELECT number, if(number % 640 = 7, 42, 0) FROM numbers(64000);

SELECT count(), sum(k) FROM t_pr_follower_skip_idx_in_order WHERE v = 42 SETTINGS enable_parallel_replicas = 0;

-- Slow down the initiator's local replica, so it does not take all the work before the followers announce.
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

SELECT count(), sum(k) FROM
(
    SELECT k FROM t_pr_follower_skip_idx_in_order WHERE v = 42 ORDER BY k
)
SETTINGS enable_parallel_replicas = 1, parallel_replicas_plan_based = 0;

SELECT count(), sum(k) FROM
(
    SELECT k FROM t_pr_follower_skip_idx_in_order WHERE v = 42 ORDER BY k DESC
)
SETTINGS enable_parallel_replicas = 1, parallel_replicas_plan_based = 0;

-- Plan-based reading would remove the inner `ORDER BY` as redundant, and the read would not be in order.
SELECT count(), sum(k) FROM
(
    SELECT k FROM t_pr_follower_skip_idx_in_order WHERE v = 42 ORDER BY k
)
SETTINGS enable_parallel_replicas = 1, parallel_replicas_plan_based = 1, query_plan_remove_redundant_sorting = 0;

-- Without the read-time skip-index evaluation, the followers read only the marks the coordinator assigns.
SET use_skip_indexes_on_data_read = 0;

SELECT count(), sum(k) FROM
(
    SELECT k FROM t_pr_follower_skip_idx_in_order WHERE v = 42 ORDER BY k
)
SETTINGS enable_parallel_replicas = 1, parallel_replicas_plan_based = 0;

SELECT count(), sum(k) FROM
(
    SELECT k FROM t_pr_follower_skip_idx_in_order WHERE v = 42 ORDER BY k DESC
)
SETTINGS enable_parallel_replicas = 1, parallel_replicas_plan_based = 0;

SELECT count(), sum(k) FROM
(
    SELECT k FROM t_pr_follower_skip_idx_in_order WHERE v = 42 ORDER BY k
)
SETTINGS enable_parallel_replicas = 1, parallel_replicas_plan_based = 1, query_plan_remove_redundant_sorting = 0;

SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

DROP TABLE t_pr_follower_skip_idx_in_order;
