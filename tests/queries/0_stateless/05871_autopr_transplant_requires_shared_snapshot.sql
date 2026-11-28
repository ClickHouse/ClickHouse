-- The plan with parallel replicas takes over the index analysis of the single-node plan, but reads with its
-- own storage snapshot. The two must be the same capture, or the ranges of one moment are read with the
-- mutations of another. With `enable_shared_storage_snapshot_in_query` the query shares one snapshot per
-- table and the plan is adopted. Without it, the plan is not even built; and where only a subquery turns it
-- off, the plan is built, but its read has a snapshot of its own and the plan is declined.

DROP TABLE IF EXISTS t_autopr_shared_snapshot;

CREATE TABLE t_autopr_shared_snapshot (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO t_autopr_shared_snapshot SELECT number FROM numbers(1000000) SETTINGS max_rows_to_read = 0;

SET enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    automatic_parallel_replicas_min_bytes_per_replica = 0, serialize_query_plan = 0, max_threads = 1,
    max_rows_to_read = 0;
-- The cost model caps the reading threads at one per `merge_tree_min_bytes_per_task_for_remote_reading` read.
SET merge_tree_min_bytes_per_task_for_remote_reading = 1024;

-- The first query of each group collects statistics, the second reaches the decision.
SELECT count() = any(500000) FROM t_autopr_shared_snapshot WHERE k < 500000
SETTINGS enable_shared_storage_snapshot_in_query = 1, log_comment = 'shared_1';
SELECT count() = any(500000) FROM t_autopr_shared_snapshot WHERE k < 500000
SETTINGS enable_shared_storage_snapshot_in_query = 1, log_comment = 'shared_2';

SELECT count() = any(500000) FROM t_autopr_shared_snapshot WHERE k < 500000
SETTINGS enable_shared_storage_snapshot_in_query = 0, log_comment = 'separate_1';
SELECT count() = any(500000) FROM t_autopr_shared_snapshot WHERE k < 500000
SETTINGS enable_shared_storage_snapshot_in_query = 0, log_comment = 'separate_2';

SELECT count() = any(500000)
FROM (SELECT k FROM t_autopr_shared_snapshot WHERE k < 500000 SETTINGS enable_shared_storage_snapshot_in_query = 0)
SETTINGS enable_shared_storage_snapshot_in_query = 1, log_comment = 'subquery_1';
SELECT count() = any(500000)
FROM (SELECT k FROM t_autopr_shared_snapshot WHERE k < 500000 SETTINGS enable_shared_storage_snapshot_in_query = 0)
SETTINGS enable_shared_storage_snapshot_in_query = 1, log_comment = 'subquery_2';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
    ProfileEvents['AutoParallelReplicasApplied'] AS applied,
    ProfileEvents['AutoParallelReplicasSkippedDueToSettings'] > 0 AS skipped_due_to_settings,
    ProfileEvents['AutoParallelReplicasPlansNotComparable'] > 0 AS not_comparable
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment IN ('shared_2', 'separate_2', 'subquery_2')
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP TABLE t_autopr_shared_snapshot;
