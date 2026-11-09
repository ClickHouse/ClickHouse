-- Functions lifted above `ORDER BY` become an `Expression` of their own. The single-node plan folds
-- `Project names` into it, which can reorder its outputs, while in the parallel replicas plan the merge
-- of the sorted streams sits between the two. Both have to key that step alike, or the plans do not match
-- and no statistics are collected.

DROP TABLE IF EXISTS t_autopr_order;

CREATE TABLE t_autopr_order (a String, b String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_autopr_order SELECT toString(number), toString(number) FROM numbers(100000);

SET enable_analyzer = 1;
SET query_plan_merge_expressions = 1;
SET query_plan_execute_functions_after_sorting = 1;
SET enable_parallel_replicas = 1;
-- Mode 2 always collects the statistics, without consulting the cost model.
SET automatic_parallel_replicas_mode = 2;
SET automatic_parallel_replicas_min_bytes_per_replica = 0;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SELECT b, upper(a) AS u FROM t_autopr_order ORDER BY b FORMAT Null SETTINGS log_comment = '05617_computed_last';
SELECT upper(a) AS u, b FROM t_autopr_order ORDER BY b FORMAT Null SETTINGS log_comment = '05617_computed_first';

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['AutoParallelReplicasPlansNotComparable'] AS not_comparable,
    ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS collected_statistics
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
  AND event_date >= yesterday() AND log_comment LIKE '05617\_%'
ORDER BY log_comment;

DROP TABLE t_autopr_order;
