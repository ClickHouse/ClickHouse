-- Automatic parallel replicas give the replicas plan the index analysis of the single-node plan, which is only correct
-- when both plans read the same state of the table. With a shared snapshot they read the same one. Without it they
-- take one each, which hold the same when nothing changed in between, so the optimization still applies. A mutation in
-- between is in 05450_autopr_transplant_snapshot_race.

DROP TABLE IF EXISTS t_autopr_snapshot;
CREATE TABLE t_autopr_snapshot (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi';
INSERT INTO t_autopr_snapshot SELECT number, intDiv(number, 1000) FROM numbers(500000);

SET enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3, automatic_parallel_replicas_mode = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1,
    automatic_parallel_replicas_min_bytes_per_replica = 1, merge_tree_min_bytes_per_task_for_remote_reading = 1,
    max_threads = 16, max_threads_min_free_memory_per_thread = 0, use_query_condition_cache = 0, serialize_query_plan = 0;

-- The first run of each collects the statistics, the second decides on them.
SELECT sum(id) FROM t_autopr_snapshot WHERE val < 100
SETTINGS log_comment = 'snapshot_shared_1', enable_shared_storage_snapshot_in_query = 1;
SELECT sum(id) FROM t_autopr_snapshot WHERE val < 100
SETTINGS log_comment = 'snapshot_shared_2', enable_shared_storage_snapshot_in_query = 1;
SELECT sum(id) FROM t_autopr_snapshot WHERE val < 200
SETTINGS log_comment = 'snapshot_own_1', enable_shared_storage_snapshot_in_query = 0;
SELECT sum(id) FROM t_autopr_snapshot WHERE val < 200
SETTINGS log_comment = 'snapshot_own_2', enable_shared_storage_snapshot_in_query = 0;

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SELECT sum(id) FROM t_autopr_snapshot WHERE val < 100;
SELECT sum(id) FROM t_autopr_snapshot WHERE val < 200;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AutoParallelReplicasApplied'] AS applied,
       ProfileEvents['AutoParallelReplicasPlansNotComparable'] AS not_comparable
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  -- `snapshot_` cannot collide with the comment the test harness sets, which starts with the test name
  AND log_comment LIKE 'snapshot_%'
  -- the replicas run sub-queries under the same comment; keep only the initiating query
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP TABLE t_autopr_snapshot;
