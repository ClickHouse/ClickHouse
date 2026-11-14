-- `parallel_replicas_allow_in_with_subquery = 0` exists because the query-based implementation has
-- every follower execute the `IN` subquery. Plan-based creates the set on the initiator, before the
-- main query, and the fragment the followers read only refers to the finished set, so the setting has
-- nothing to protect against there - and switching it off used to cost the set's own read its
-- parallel replicas for no reason.

DROP TABLE IF EXISTS t_main_05432;
DROP TABLE IF EXISTS t_set_05432;
CREATE TABLE t_main_05432 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_set_05432 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_main_05432 SELECT number, number % 100 FROM numbers(50000);
INSERT INTO t_set_05432 SELECT number FROM numbers(5);

SET enable_analyzer = 1, automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 1,
    max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 0;

SELECT count() FROM t_main_05432 WHERE y IN (SELECT k FROM t_set_05432)
SETTINGS parallel_replicas_allow_in_with_subquery = 0, log_comment = '05432_in_with_subquery_off';

SELECT count() FROM t_main_05432 WHERE y IN (SELECT k FROM t_set_05432)
SETTINGS parallel_replicas_allow_in_with_subquery = 1, log_comment = '05432_in_with_subquery_on';

SYSTEM FLUSH LOGS query_log;

-- Two reads with parallel replicas either way: the main one and the set's own.
SELECT log_comment, ProfileEvents['ParallelReplicasQueryCount']
FROM system.query_log
WHERE log_comment IN ('05432_in_with_subquery_off', '05432_in_with_subquery_on')
  AND type = 'QueryFinish' AND current_database = currentDatabase() AND is_initial_query
  AND event_date >= yesterday() AND event_time >= now() - INTERVAL 30 MINUTE
ORDER BY log_comment, event_time_microseconds DESC LIMIT 1 BY log_comment;

DROP TABLE t_main_05432;
DROP TABLE t_set_05432;
