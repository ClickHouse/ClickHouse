-- `additional_table_filters` used to switch parallel replicas off, because a follower re-resolving
-- the setting against its own current database matches the wrong table. Plan-based parallel replicas
-- ships the plan with the filter already lowered into it, so the follower resolves nothing and the
-- combination works.

DROP TABLE IF EXISTS t_05431;
CREATE TABLE t_05431 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_05431 SELECT number, number % 10 FROM numbers(50000);

SET enable_analyzer = 1, automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 1,
    max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_plan_based = 1, serialize_query_plan = 0;

-- `parallel_replicas_local_plan = 0` so every row comes from a replica reading the shipped fragment,
-- which is what makes the filter's result below say that the filter travelled with it.
SELECT count() FROM t_05431
SETTINGS parallel_replicas_local_plan = 0, additional_table_filters = {'t_05431': 'y < 5'},
         log_comment = '05431_pr_plan_based_additional_table_filters';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['ParallelReplicasQueryCount'] > 0
FROM system.query_log
WHERE log_comment = '05431_pr_plan_based_additional_table_filters' AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND is_initial_query
  AND event_date >= yesterday() AND event_time >= now() - INTERVAL 30 MINUTE
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_05431;
