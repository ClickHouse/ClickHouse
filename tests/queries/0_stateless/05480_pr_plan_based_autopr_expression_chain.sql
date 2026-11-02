-- Automatic parallel replicas must match the plan with parallel replicas to the single-node one when the two differ
-- only in how their expressions are merged. The local plan of plan-based parallel replicas is optimized once more,
-- and that merges expressions the single-node plan keeps apart, and keeps apart some it merges:
-- - an INNER JOIN with a join runtime filter: `Left Pre Join Actions` stays apart in the local plan;
-- - a filter moved to PREWHERE: the expression it leaves is merged into `Before GROUP BY` in the local plan.

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1, parallel_replicas_index_analysis_only_on_coordinator = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_plan_based = 1;
-- Keep the parallelized side oriented as written (the randomizer may flip this).
SET query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0;
-- The probe side (20000 rows) exceeds the threshold, so the runtime filter is added in both plan builds.
SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 1000;
SET parallel_replicas_allow_in_with_subquery = 1, optimize_move_to_prewhere = 1;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET use_query_condition_cache = 0, serialize_query_plan = 0;

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;
DROP TABLE IF EXISTS t_group;

CREATE TABLE t_left (key UInt64, payload String) ENGINE = MergeTree ORDER BY key
AS SELECT number, toString(cityHash64(number)) FROM numbers(100000);
CREATE TABLE t_right (key UInt64) ENGINE = MergeTree ORDER BY key
AS SELECT number FROM numbers(20000);
CREATE TABLE t_group (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
AS SELECT number, number FROM numbers(100000);

SELECT t1.payload FROM t_left AS t1 INNER JOIN t_right AS t2 USING (key) FORMAT Null
SETTINGS log_comment = '05480_join_runtime_filter';

SELECT k % 10, sum(v) FROM t_group WHERE k IN (SELECT number * 2 FROM numbers(50000)) GROUP BY k % 10 FORMAT Null
SETTINGS log_comment = '05480_prewhere_group_by';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AutoParallelReplicasPlansNotComparable'] AS not_comparable,
       ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS output_bytes_collected
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE
  AND current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
  AND log_comment IN ('05480_join_runtime_filter', '05480_prewhere_group_by')
ORDER BY log_comment;

DROP TABLE t_left;
DROP TABLE t_right;
DROP TABLE t_group;
