-- A read of projection parts has a storage snapshot of its own. When such a read lies outside the subtree the
-- decision is matched on, here on the other side of a `JOIN`, the plans still match, and the transplant of the
-- index analysis must decline the projection read before it compares the snapshots. Both kinds of projection
-- read are covered: one chosen by the projection optimization, which comes with its index analysis, and one of
-- `mergeTreeProjection`, whose analysis is lazy.

DROP TABLE IF EXISTS t_autopr_proj_left;
DROP TABLE IF EXISTS t_autopr_proj_right;

CREATE TABLE t_autopr_proj_left (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO t_autopr_proj_left SELECT number, number % 100 FROM numbers(1000000) SETTINGS max_rows_to_read = 0;

CREATE TABLE t_autopr_proj_right (a UInt64, b UInt64, PROJECTION by_b (SELECT * ORDER BY b))
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 128;
INSERT INTO t_autopr_proj_right SELECT number, number % 1000 FROM numbers(100000) SETTINGS max_rows_to_read = 0;

SET enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    automatic_parallel_replicas_min_bytes_per_replica = 0, serialize_query_plan = 0, max_threads = 1,
    max_rows_to_read = 0, optimize_use_projections = 1, enable_join_runtime_filters = 0,
    query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0;
-- The cost model caps the reading threads at one per `merge_tree_min_bytes_per_task_for_remote_reading` read.
SET merge_tree_min_bytes_per_task_for_remote_reading = 1024;

-- The first query collects statistics, the second reaches the decision.
SELECT count()
FROM (SELECT v, sum(k) FROM t_autopr_proj_left WHERE k < 500000 GROUP BY v) AS l
JOIN (SELECT a, b FROM t_autopr_proj_right WHERE b = 7) AS r ON l.v = r.a
SETTINGS log_comment = 'projection_join_1';
SELECT count()
FROM (SELECT v, sum(k) FROM t_autopr_proj_left WHERE k < 500000 GROUP BY v) AS l
JOIN (SELECT a, b FROM t_autopr_proj_right WHERE b = 7) AS r ON l.v = r.a
SETTINGS log_comment = 'projection_join_2';

SELECT count()
FROM (SELECT v, sum(k) FROM t_autopr_proj_left WHERE k < 500000 GROUP BY v) AS l
JOIN (SELECT a, b FROM mergeTreeProjection(currentDatabase(), t_autopr_proj_right, by_b) WHERE b = 7) AS r ON l.v = r.a
SETTINGS query_plan_optimize_join_order_limit = 0, log_comment = 'table_function_join_1';
SELECT count()
FROM (SELECT v, sum(k) FROM t_autopr_proj_left WHERE k < 500000 GROUP BY v) AS l
JOIN (SELECT a, b FROM mergeTreeProjection(currentDatabase(), t_autopr_proj_right, by_b) WHERE b = 7) AS r ON l.v = r.a
SETTINGS query_plan_optimize_join_order_limit = 0, log_comment = 'table_function_join_2';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
    ProfileEvents['AutoParallelReplicasApplied'] AS applied,
    ProfileEvents['AutoParallelReplicasPlansNotComparable'] > 0 AS not_comparable,
    has(projections, currentDatabase() || '.t_autopr_proj_right.by_b') AS used_projection_optimization
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment IN ('projection_join_2', 'table_function_join_2')
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP TABLE t_autopr_proj_left;
DROP TABLE t_autopr_proj_right;
