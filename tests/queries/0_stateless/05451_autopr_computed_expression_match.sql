-- Automatic parallel replicas give the replicas plan the index analysis of the single-node plan when the two match. A
-- computed expression that keeps the row count and the column types is invisible to the statistics, but a filter
-- above it is pushed down to the read through it, so the plans must not match when it computes something else. Here
-- `getSetting('automatic_parallel_replicas_mode')` differs between the plans, and so does `x` in terms of `k`.

DROP TABLE IF EXISTS t_autopr_computed;
CREATE TABLE t_autopr_computed (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi';
INSERT INTO t_autopr_computed SELECT number FROM numbers(1000000);

SET enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, automatic_parallel_replicas_mode = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1,
    automatic_parallel_replicas_min_bytes_per_replica = 1, merge_tree_min_bytes_per_task_for_remote_reading = 1,
    max_threads = 16, max_threads_min_free_memory_per_thread = 0, use_query_condition_cache = 0, serialize_query_plan = 0,
    enable_shared_storage_snapshot_in_query = 1, query_plan_merge_expressions = 0;

-- The first run collects the statistics, the second decides on them.
SELECT count(), sum(x)
FROM (SELECT k + 100000 * (1 + getSetting('automatic_parallel_replicas_mode')) AS x FROM t_autopr_computed)
WHERE x < 700000
SETTINGS log_comment = 'computed_setting_1';
SELECT count(), sum(x)
FROM (SELECT k + 100000 * (1 + getSetting('automatic_parallel_replicas_mode')) AS x FROM t_autopr_computed)
WHERE x < 700000
SETTINGS log_comment = 'computed_setting_2';

-- The same with a literal, which both plans agree on: the optimization applies.
SELECT count(), sum(x) FROM (SELECT k + 200000 AS x FROM t_autopr_computed) WHERE x < 700000
SETTINGS log_comment = 'computed_literal_1';
SELECT count(), sum(x) FROM (SELECT k + 200000 AS x FROM t_autopr_computed) WHERE x < 700000
SETTINGS log_comment = 'computed_literal_2';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AutoParallelReplicasApplied'] AS applied,
       ProfileEvents['AutoParallelReplicasPlansNotComparable'] AS not_comparable
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  -- `computed_` cannot collide with the comment the test harness sets, which starts with the test name
  AND log_comment LIKE 'computed_%'
  -- the replicas run sub-queries under the same comment; keep only the initiating query
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP TABLE t_autopr_computed;
