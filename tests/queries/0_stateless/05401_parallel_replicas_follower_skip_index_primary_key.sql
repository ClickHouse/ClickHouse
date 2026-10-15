-- A parallel replicas follower skips its own index analysis, so the input of its read-time skip-index
-- evaluation must still be narrowed by the primary key: it must not select more marks than a single-node read.

SET automatic_parallel_replicas_mode = 0;
SET use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
-- The CI profile's max_rows_to_read with read_overflow_mode = 'throw' turns the data-read path off.
SET max_rows_to_read = 0, max_rows_to_read_leaf = 0;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_pr_follower_skip_idx;

CREATE TABLE t_pr_follower_skip_idx (p UInt8, k UInt64, v UInt64, INDEX iv v TYPE minmax GRANULARITY 1)
ENGINE = MergeTree PARTITION BY p ORDER BY k
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

-- 10 parts of 100 granules: k = 3 is granules 30..39 of each part, v = 42 only in granules 30..31,
-- v = 0 in the rest of k = 3, and v alternates 0/100 everywhere else (the minmax index keeps those).
INSERT INTO t_pr_follower_skip_idx
SELECT intDiv(number, 6400) AS p, intDiv(number % 6400, 640) AS k, if(k = 3, if(number % 640 < 128, 42, 0), (number % 2) * 100)
FROM numbers(64000);

SELECT sum(v), sum(k) FROM t_pr_follower_skip_idx WHERE k = 3 AND v = 42
SETTINGS enable_parallel_replicas = 0, log_comment = '05401_single';

SELECT sum(v), sum(k) FROM t_pr_follower_skip_idx WHERE sleepEachRow(0.001) = 0 AND k = 3 AND v = 42
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_local_plan = 1, parallel_replicas_index_analysis_only_on_coordinator = 1,
    parallel_replicas_prefer_local_replica = 1, parallel_replicas_plan_based = 0, max_threads = 2,
    function_sleep_max_microseconds_per_block = 0, log_comment = '05401_query_based';

SELECT sum(v), sum(k) FROM t_pr_follower_skip_idx WHERE sleepEachRow(0.001) = 0 AND k = 3 AND v = 42
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_local_plan = 1, parallel_replicas_index_analysis_only_on_coordinator = 1,
    parallel_replicas_prefer_local_replica = 1, parallel_replicas_plan_based = 1, max_threads = 2,
    function_sleep_max_microseconds_per_block = 0, log_comment = '05401_plan_based';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['SelectedMarks'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = '05401_single';

SELECT
    replaceOne(log_comment, '05401_', '') AS mode,
    max(ProfileEvents['SelectedMarks']) <= (
        SELECT ProfileEvents['SelectedMarks'] FROM system.query_log
        WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = '05401_single')
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND NOT is_initial_query
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query
            AND log_comment IN ('05401_query_based', '05401_plan_based'))
GROUP BY mode
ORDER BY mode;

DROP TABLE t_pr_follower_skip_idx;
