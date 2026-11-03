-- The statistics key of automatic parallel replicas must tell apart two expressions that compute different values with
-- the same types. `left(s, 1)` and `hex(s)` both turn a `String` into a `String`, and used to be keyed as if they did
-- nothing, so `hex(s)` reused the statistics of `left(s, 1)`: an output about five times too small, which can enable
-- parallel replicas for a query they make slower.
--
-- The check, as in 05292: a query whose statistics are already cached does not collect them again
-- (`RuntimeDataflowStatisticsOutputBytes` stays 0). So `left(s, 1)` collects and its repeat does not, and `hex(s)`
-- must collect its own.

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1, parallel_replicas_local_plan = 1, parallel_replicas_index_analysis_only_on_coordinator = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas';
SET automatic_parallel_replicas_min_bytes_per_replica = 0;
SET merge_tree_min_bytes_per_task_for_remote_reading = 0;
SET max_threads = 4, max_block_size = 128;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_05493;

CREATE TABLE t_05493 (k UInt64, s String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
-- A merge would change the read hash and force a recollection, which would mask a shared key.
SYSTEM STOP MERGES t_05493;
INSERT INTO t_05493 SELECT number, toString(cityHash64(number)) FROM numbers(25000);

SELECT left(s, 1) FROM t_05493 FORMAT Null SETTINGS log_comment = 'q05493_left_0';
SELECT left(s, 1) FROM t_05493 FORMAT Null SETTINGS log_comment = 'q05493_left_1';
SELECT hex(s) FROM t_05493 FORMAT Null SETTINGS log_comment = 'q05493_hex_0';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment AS query, ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS stats_collected
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - toIntervalMinute(15))
  AND (current_database = currentDatabase()) AND (log_comment LIKE 'q05493\_%') AND (type = 'QueryFinish')
ORDER BY event_time_microseconds
FORMAT TSVWithNames;

DROP TABLE t_05493;
