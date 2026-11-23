-- `automatic_parallel_replicas_statistics_drift_ratio` decides how far the rows a read selects may move from the ones
-- its statistics were collected on before they are collected again. The table grows by 1.75x between the runs.

DROP TABLE IF EXISTS t_autopr_drift_ratio;

CREATE TABLE t_autopr_drift_ratio (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_autopr_drift_ratio SELECT number, number FROM numbers(100000);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    automatic_parallel_replicas_min_bytes_per_replica = 0, serialize_query_plan = 0;
-- An aggregation that spills to disk merges on a path that collects no statistics.
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
-- The query condition cache would change the rows the analysis selects between the runs.
SET use_query_condition_cache = 0;

-- Collects the statistics on 100000 rows.
SELECT k % 10, sum(v) FROM t_autopr_drift_ratio GROUP BY k % 10 FORMAT Null SETTINGS log_comment = 'drift_ratio_1_collect';

INSERT INTO t_autopr_drift_ratio SELECT number + 100000, number FROM numbers(75000);

-- 175000 rows: within a ratio of 2, so the statistics are decided on.
SELECT k % 10, sum(v) FROM t_autopr_drift_ratio GROUP BY k % 10 FORMAT Null
SETTINGS automatic_parallel_replicas_statistics_drift_ratio = 2, log_comment = 'drift_ratio_2_within_2';
-- Beyond the default ratio of 1.5, so they are collected again.
SELECT k % 10, sum(v) FROM t_autopr_drift_ratio GROUP BY k % 10 FORMAT Null SETTINGS log_comment = 'drift_ratio_3_beyond_default';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AutoParallelReplicasNoStatistics'] AS no_statistics,
       ProfileEvents['AutoParallelReplicasStatisticsDrifted'] AS drifted
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  -- `drift_ratio_` cannot collide with the comment the test harness sets, which starts with the test name
  AND log_comment LIKE 'drift_ratio_%'
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP TABLE t_autopr_drift_ratio;
