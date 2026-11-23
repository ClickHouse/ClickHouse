-- When counting the compressed blocks read takes more ranges than `automatic_parallel_replicas_max_counted_ranges`,
-- the statistics record that the input is not measured, and the next run declines parallel replicas from them without
-- collecting the statistics again.

DROP TABLE IF EXISTS t_autopr_input_ranges;

CREATE TABLE t_autopr_input_ranges (key UInt64, value String) ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_autopr_input_ranges SELECT number, toString(number) FROM numbers(100000);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET automatic_parallel_replicas_min_bytes_per_replica = 0;
-- The two runs must select the same granules, see `05332_autopr_min_bytes_per_replica_events`.
SET use_query_condition_cache = 0;

-- The first counted file takes the only unit, so its ranges do not fit.
SELECT key % 10 AS k, max(value) FROM t_autopr_input_ranges GROUP BY k FORMAT Null
SETTINGS automatic_parallel_replicas_max_counted_ranges = 1, log_comment = 'autopr_input_ranges_collect';
SELECT key % 10 AS k, max(value) FROM t_autopr_input_ranges GROUP BY k FORMAT Null
SETTINGS automatic_parallel_replicas_max_counted_ranges = 1, log_comment = 'autopr_input_ranges_rejected';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AutoParallelReplicasNoStatistics'] AS collected,
       ProfileEvents['AutoParallelReplicasRejectedByUnmeasuredInput'] AS rejected,
       ProfileEvents['AutoParallelReplicasCostModelEvaluated'] AS evaluated,
       ProfileEvents['AutoParallelReplicasApplied'] AS applied
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND startsWith(log_comment, 'autopr_input_ranges_')
    AND (type = 'QueryFinish') AND is_initial_query
ORDER BY event_time_microseconds;

DROP TABLE t_autopr_input_ranges;
