-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS autopr_join_width_wide;
DROP TABLE IF EXISTS autopr_join_width_narrow;

CREATE TABLE autopr_join_width_wide (p UInt64, k UInt64, payload String)
ENGINE = MergeTree PARTITION BY p ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_level_for_wide_part = 0;
CREATE TABLE autopr_join_width_narrow (k UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO autopr_join_width_wide SELECT 0, number, '' FROM numbers(2000000);
INSERT INTO autopr_join_width_wide SELECT 1, number, repeat(toString(number), 30) FROM numbers(100000);
INSERT INTO autopr_join_width_narrow SELECT number FROM numbers(250000);

SET enable_join_runtime_filters = 0, query_plan_optimize_join_order_randomize = 0, query_plan_optimize_join_order_limit = 2,
    use_statistics = 1, use_statistics_cache = 1, max_threads = 1, merge_tree_min_bytes_per_task_for_remote_reading = 1024,
    automatic_parallel_replicas_min_bytes_per_replica = 0, automatic_parallel_replicas_max_duplicated_read_ratio = 1,
    parallel_replicas_plan_based = 0, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0, max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- The probe must not use the pruned partition's short strings when choosing the build side.
SELECT payload FROM autopr_join_width_wide AS w INNER ALL JOIN autopr_join_width_narrow AS n USING (k)
WHERE w.p = 1
SETTINGS query_plan_join_swap_table = 'auto', log_comment = '05872_autopr_join_input_width'
FORMAT Null;

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['AutoParallelReplicasPlansNotComparable'],
    ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 OR ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
    AND event_date >= yesterday() AND event_time > now() - INTERVAL 10 MINUTE
    AND log_comment = '05872_autopr_join_input_width'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE autopr_join_width_wide;
DROP TABLE autopr_join_width_narrow;
