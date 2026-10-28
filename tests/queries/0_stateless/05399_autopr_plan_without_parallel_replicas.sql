-- When the planner turns parallel replicas off by itself while building the plan with parallel replicas, the
-- plan has no read from the other replicas. That is counted in `AutoParallelReplicasPlanWithoutParallelReplicas`,
-- apart from `AutoParallelReplicasPlansNotComparable`, which is for plans this optimization cannot handle.

DROP TABLE IF EXISTS t_autopr_plan_without_pr;

CREATE TABLE t_autopr_plan_without_pr (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_autopr_plan_without_pr SELECT number, number FROM numbers(100000);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    automatic_parallel_replicas_min_bytes_per_replica = 0, serialize_query_plan = 0;

-- The planner does not allow `IN` with a subquery, and plans the read locally.
SELECT k % 10, sum(v) FROM t_autopr_plan_without_pr WHERE k IN (SELECT number * 2 FROM numbers(50000)) GROUP BY k % 10
FORMAT Null SETTINGS parallel_replicas_allow_in_with_subquery = 0, log_comment = 'plan_without_pr_declined';

-- The same query with `IN` with a subquery allowed reads from the other replicas, and goes on to collect statistics.
SELECT k % 10, sum(v) FROM t_autopr_plan_without_pr WHERE k IN (SELECT number * 2 FROM numbers(50000)) GROUP BY k % 10
FORMAT Null SETTINGS parallel_replicas_allow_in_with_subquery = 1, log_comment = 'plan_without_pr_used';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] AS built,
       ProfileEvents['AutoParallelReplicasPlanWithoutParallelReplicas'] AS without_parallel_replicas,
       ProfileEvents['AutoParallelReplicasPlansNotComparable'] AS not_comparable,
       ProfileEvents['AutoParallelReplicasNoStatistics'] AS no_statistics
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  -- `plan_without_pr_` cannot collide with the comment the test harness sets, which starts with the test name
  AND log_comment LIKE 'plan_without_pr_%'
  -- the replicas run sub-queries under the same comment; keep only the initiating query
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP TABLE t_autopr_plan_without_pr;
