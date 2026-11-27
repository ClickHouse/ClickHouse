-- Tags: no-parallel, no-fasttest
-- no-parallel: the failpoint is process-wide.

-- A filtered aggregation answered fully from statistics replaces the initiator's read with a prepared
-- source. A follower forced to skip the optimization must still be able to request its read stream.

DROP TABLE IF EXISTS t_pr_filtered_stats;
CREATE TABLE t_pr_filtered_stats
(
    id UInt64,
    val Int32
)
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'basic', index_granularity = 32;

SET materialize_statistics_on_insert = 1;
INSERT INTO t_pr_filtered_stats SELECT number, toInt32(number % 1000) - 500 FROM numbers(5000);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;
SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0;
SET aggregate_functions_null_for_empty = 0;
SET use_statistics_for_part_pruning = 1;
SET use_statistics_for_min_max_aggregation = 1, use_statistics_for_sum_avg_aggregation = 1;

SELECT count() > 0 FROM (
    EXPLAIN SELECT count(), sum(val), min(val), max(val) FROM t_pr_filtered_stats
    WHERE val >= -500 AND val <= 499
) WHERE explain ILIKE '%_statistics_projection%';

SYSTEM ENABLE FAILPOINT parallel_replicas_skip_aggregate_projection_on_follower;
SELECT count(), sum(val), min(val), max(val) FROM t_pr_filtered_stats
WHERE val >= -500 AND val <= 499
SETTINGS log_comment = '05877_filtered_stats';
SYSTEM DISABLE FAILPOINT parallel_replicas_skip_aggregate_projection_on_follower;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ParallelReplicasHandleRequestMicroseconds'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '05877_filtered_stats'
  AND type = 'QueryFinish'
  AND query_id = initial_query_id
  AND event_time >= now() - INTERVAL 600 SECOND;

DROP TABLE t_pr_filtered_stats;
