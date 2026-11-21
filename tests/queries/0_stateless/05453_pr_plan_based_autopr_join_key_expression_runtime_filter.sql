-- Automatic parallel replicas must match the plan with parallel replicas to the single-node one for a join
-- whose right key is an expression and which gets a join runtime filter. The filter computes the key below
-- the join, which leaves the columns it is computed from unused. The plan-based implementation optimizes its
-- local plan again from the first pass and removed them, the single-node plan kept them, so the two plans
-- differed and no statistics were collected.

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';
SET parallel_replicas_prefer_local_join=1;
-- Keep the parallelized side oriented as written (the randomizer may flip this).
SET query_plan_join_swap_table='false';
-- The probe side (20000 rows) exceeds the threshold, so the runtime filter is added in both plan builds.
SET query_plan_optimize_join_order_randomize=0, enable_join_runtime_filters=1, join_runtime_filter_min_probe_rows=1000;
SET query_plan_remove_unused_columns=1;
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;
SET use_query_condition_cache=0;

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left (key UInt64, payload String) ENGINE = MergeTree ORDER BY key
AS SELECT number, toString(cityHash64(number)) FROM numbers(100000);
CREATE TABLE t_right (key UInt64) ENGINE = MergeTree ORDER BY key
AS SELECT number FROM numbers(20000);

SELECT t1.payload FROM t_left AS t1 INNER JOIN (SELECT key, toUInt64(1) AS m FROM t_right) AS t2 ON t1.key = t2.key + t2.m FORMAT Null
SETTINGS parallel_replicas_plan_based=0, log_comment='05453_query_based';
SELECT t1.payload FROM t_left AS t1 INNER JOIN (SELECT key, toUInt64(2) AS m FROM t_right) AS t2 ON t1.key = t2.key + t2.m FORMAT Null
SETTINGS parallel_replicas_plan_based=1, log_comment='05453_plan_based';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AutoParallelReplicasPlansNotComparable'] AS not_comparable,
       ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS output_bytes_collected
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
  AND (current_database = currentDatabase()) AND (log_comment IN ('05453_query_based', '05453_plan_based')) AND (type = 'QueryFinish')
ORDER BY log_comment;

DROP TABLE t_left;
DROP TABLE t_right;
