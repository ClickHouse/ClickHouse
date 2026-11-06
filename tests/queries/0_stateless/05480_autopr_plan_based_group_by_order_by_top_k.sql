-- A plan-based parallel replicas plan splits the aggregation into the partial one and the merge, and ships the partial
-- one to the replicas. Both the local and the shipped partial aggregation must get the `GROUP BY` top-K heap of the
-- single-node aggregation: automatic parallel replicas matches the two plans, and prices the output of each replica by
-- the output of the single-node aggregation.

DROP TABLE IF EXISTS t_05480;
CREATE TABLE t_05480 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_05480 SELECT intHash64(number) % 100000, number FROM numbers(1000000);

SET parallel_replicas_plan_based = 1, parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    automatic_parallel_replicas_min_bytes_per_replica = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET enable_analyzer = 1, query_plan_enable_optimizations = 1, enable_group_by_top_k_optimization = 1,
    max_rows_to_group_by = 0, exact_rows_before_limit = 0, query_plan_max_limit_for_top_k_optimization = 1000;
-- The replicas aggregate in order under these, which takes no heap.
SET enable_memory_bound_merging_of_aggregation_results = 0, optimize_aggregation_in_order = 0;
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0;
-- Statistics of a previous run can take the heap away from the aggregation.
SET collect_hash_table_stats_during_aggregation = 0;
-- The top-K boundary of a run can leave an entry in the query condition cache, so that the next run reads fewer rows,
-- which looks like changed data and recollects the statistics instead of pricing.
SET use_query_condition_cache = 0;

-- Without the local replica, the remote ones read everything, and skip rows in the heap. The groups are the same as
-- without parallel replicas.
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k LIMIT 3 SETTINGS enable_parallel_replicas = 0;
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k LIMIT 3
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 0,
    log_comment = 'q05480_remote_asc';
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k DESC LIMIT 3 SETTINGS enable_parallel_replicas = 0;
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k DESC LIMIT 3
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 0,
    log_comment = 'q05480_remote_desc';

SET enable_parallel_replicas = 1, parallel_replicas_local_plan = 1;

-- Mode 2 only matches the plans.
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k LIMIT 10
FORMAT Null SETTINGS automatic_parallel_replicas_mode = 2, enable_group_by_top_k_dynamic_filtering = 0,
    log_comment = 'q05480_match_asc';
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k DESC LIMIT 10
FORMAT Null SETTINGS automatic_parallel_replicas_mode = 2, enable_group_by_top_k_dynamic_filtering = 0,
    log_comment = 'q05480_match_desc';
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k LIMIT 10
FORMAT Null SETTINGS automatic_parallel_replicas_mode = 2, enable_group_by_top_k_dynamic_filtering = 1,
    log_comment = 'q05480_match_asc_dynamic_filtering';

-- Mode 1 prices the query with the statistics that mode 2 collected above.
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k LIMIT 10
FORMAT Null SETTINGS automatic_parallel_replicas_mode = 1, enable_group_by_top_k_dynamic_filtering = 1,
    log_comment = 'q05480_priced';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log, text_log;

SELECT q.log_comment, sum(r.ProfileEvents['AggregationTopKRowsSkipped']) > 0
FROM system.query_log AS r
INNER JOIN
(
    SELECT query_id, log_comment
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
        AND (current_database = currentDatabase()) AND startsWith(log_comment, 'q05480_remote_')
        AND (type = 'QueryFinish') AND is_initial_query
) AS q ON r.initial_query_id = q.query_id
WHERE (r.event_date >= yesterday()) AND (r.event_time >= NOW() - INTERVAL '15 MINUTES')
    AND (r.type = 'QueryFinish') AND NOT r.is_initial_query
GROUP BY q.log_comment
ORDER BY q.log_comment;

SELECT log_comment,
       ProfileEvents['AutoParallelReplicasPlansNotComparable'] AS not_comparable,
       ProfileEvents['AutoParallelReplicasNoStatistics'] AS no_statistics
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
    AND (current_database = currentDatabase()) AND startsWith(log_comment, 'q05480_match_')
    AND (type = 'QueryFinish') AND is_initial_query
ORDER BY log_comment;

SELECT count()
FROM system.text_log AS t
INNER JOIN
(
    SELECT query_id
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
        AND (current_database = currentDatabase()) AND (log_comment = 'q05480_priced') AND (type = 'QueryFinish')
        AND is_initial_query
) AS q ON t.query_id = q.query_id
WHERE (t.event_date >= yesterday()) AND (t.event_time >= NOW() - INTERVAL '15 MINUTES')
    AND (t.logger_name = 'AutoParallelReplicas') AND (t.message LIKE 'The applied formula%')
SETTINGS max_rows_to_read = 0; -- system.text_log can be really big

DROP TABLE t_05480;
