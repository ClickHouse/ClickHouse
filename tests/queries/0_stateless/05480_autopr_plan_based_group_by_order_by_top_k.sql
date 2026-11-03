-- A plan-based parallel replicas plan splits the aggregation into the partial one and the merge after planning, so its
-- local partial aggregation must get the same `GROUP BY` top-K heap as the single-node aggregation. Otherwise automatic
-- parallel replicas cannot match the two plans, and never collects statistics for the query.

DROP TABLE IF EXISTS t_05480;
CREATE TABLE t_05480 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_05480 SELECT intHash64(number) % 100000, number FROM numbers(1000000);

-- Mode 2 only matches the plans.
SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    parallel_replicas_plan_based = 1, parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    automatic_parallel_replicas_min_bytes_per_replica = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET enable_analyzer = 1, query_plan_enable_optimizations = 1, enable_group_by_top_k_optimization = 1,
    max_rows_to_group_by = 0, exact_rows_before_limit = 0, query_plan_max_limit_for_top_k_optimization = 1000;
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0;
-- Statistics of a previous run can take the heap away from the aggregation.
SET collect_hash_table_stats_during_aggregation = 0;

SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k LIMIT 10
FORMAT Null SETTINGS enable_group_by_top_k_dynamic_filtering = 0, log_comment = 'q05480_asc';
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k DESC LIMIT 10
FORMAT Null SETTINGS enable_group_by_top_k_dynamic_filtering = 0, log_comment = 'q05480_desc';
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k LIMIT 10
FORMAT Null SETTINGS enable_group_by_top_k_dynamic_filtering = 1, log_comment = 'q05480_asc_dynamic_filtering';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AutoParallelReplicasPlansNotComparable'] AS not_comparable,
       ProfileEvents['AutoParallelReplicasNoStatistics'] AS no_statistics
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
    AND (current_database = currentDatabase()) AND startsWith(log_comment, 'q05480_')
    AND (type = 'QueryFinish') AND is_initial_query
ORDER BY log_comment;

DROP TABLE t_05480;
