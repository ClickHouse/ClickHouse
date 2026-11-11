-- A filter on the current time must not keep automatic parallel replicas from reusing statistics: a
-- folded `now64()` is identified by the expression it was folded from, not by the value, which differs
-- between two executions and even between the two plans of one query. Windows of different length must
-- still be told apart.

DROP TABLE IF EXISTS t_autopr_now;

CREATE TABLE t_autopr_now (k UInt64, ts DateTime64(3)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_autopr_now SELECT number, now64(3) - toIntervalSecond(number) FROM numbers(100000);

SET enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    automatic_parallel_replicas_min_bytes_per_replica = 0, serialize_query_plan = 0;
-- An aggregation that spills to disk merges on a path that collects no statistics.
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT k % 10, count() FROM t_autopr_now WHERE ts >= now64(3) - INTERVAL 1 DAY GROUP BY k % 10
FORMAT Null SETTINGS log_comment = 'now_1_day_1';
SELECT k % 10, count() FROM t_autopr_now WHERE ts >= now64(3) - INTERVAL 1 DAY GROUP BY k % 10
FORMAT Null SETTINGS log_comment = 'now_1_day_2';
SELECT k % 10, count() FROM t_autopr_now WHERE ts >= now64(3) - INTERVAL 1 HOUR GROUP BY k % 10
FORMAT Null SETTINGS log_comment = 'now_1_hour';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- The first run collects, the second reuses what it collected, and the shorter window collects its own.
SELECT log_comment,
       ProfileEvents['AutoParallelReplicasPlansNotComparable'] AS not_comparable,
       ProfileEvents['AutoParallelReplicasNoStatistics'] AS no_statistics,
       ProfileEvents['AutoParallelReplicasCostModelEvaluated'] AS cost_model_evaluated
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  -- `now_` cannot collide with the comment the test harness sets, which starts with the test name
  AND log_comment LIKE 'now_%'
  -- the replicas run sub-queries under the same comment; keep only the initiating query
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP TABLE t_autopr_now;
