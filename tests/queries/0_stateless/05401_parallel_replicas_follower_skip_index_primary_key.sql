-- Tags: no-parallel, no-fasttest
-- Tag no-parallel: the failpoint is server-wide.
-- Tag no-fasttest: tests that must run alone are kept out of the fast test.
-- Random settings limits: use_lightweight_primary_key_index_analysis=(1, None)
-- A parallel replicas follower skips its own index analysis, so the input of its read-time skip-index
-- evaluation must be the ranges analyzed on the initiator: it must not select more marks than a single-node read.

SET automatic_parallel_replicas_mode = 0;
SET use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
-- The CI profile's max_rows_to_read with read_overflow_mode = 'throw' turns the data-read path off.
SET max_rows_to_read = 0, max_rows_to_read_leaf = 0;
SET use_query_condition_cache = 0;
SET use_partition_minmax_for_primary_key_pruning = 1;

DROP TABLE IF EXISTS t_pr_follower_skip_idx;

CREATE TABLE t_pr_follower_skip_idx (p UInt8, k UInt64, v UInt64, INDEX iv v TYPE minmax GRANULARITY 1)
ENGINE = MergeTree PARTITION BY p ORDER BY (k, p)
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    add_minmax_index_for_numeric_columns = 0;

-- 10 parts of 100 granules, granule g of each part holds k = g: v = 42 only in granules 30..31, v = 0 in 32..39, and v
-- alternates 0/100 elsewhere (the minmax index keeps those). No part has p = 100, and only the part's partition minmax
-- bound lets the primary key rule that branch out, as on the initiator.
INSERT INTO t_pr_follower_skip_idx
SELECT intDiv(number, 6400) AS p, intDiv(number % 6400, 64) AS k, if(k BETWEEN 30 AND 39, if(k < 32, 42, 0), (number % 2) * 100)
FROM numbers(64000);

SELECT sum(v), sum(k) FROM t_pr_follower_skip_idx WHERE (k BETWEEN 30 AND 39 OR p = 100) AND v = 42
SETTINGS enable_parallel_replicas = 0, log_comment = '05401_single';

-- Slow down the initiator's local replica, so it does not take all the work before the followers announce.
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

SELECT sum(v), sum(k) FROM t_pr_follower_skip_idx WHERE (k BETWEEN 30 AND 39 OR p = 100) AND v = 42
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_local_plan = 1, parallel_replicas_index_analysis_only_on_coordinator = 1,
    parallel_replicas_plan_based = 0, max_threads = 2, log_comment = '05401_query_based';

SELECT sum(v), sum(k) FROM t_pr_follower_skip_idx WHERE (k BETWEEN 30 AND 39 OR p = 100) AND v = 42
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_local_plan = 1, parallel_replicas_index_analysis_only_on_coordinator = 1,
    parallel_replicas_plan_based = 1, max_threads = 2, log_comment = '05401_plan_based';

-- Without the read-time skip-index evaluation, the followers read only the marks the coordinator assigns.
SELECT sum(v), sum(k) FROM t_pr_follower_skip_idx WHERE (k BETWEEN 30 AND 39 OR p = 100) AND v = 42
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_local_plan = 1, parallel_replicas_index_analysis_only_on_coordinator = 1,
    parallel_replicas_plan_based = 0, max_threads = 2, use_skip_indexes_on_data_read = 0, log_comment = '05401_query_based_no_data_read';

SELECT sum(v), sum(k) FROM t_pr_follower_skip_idx WHERE (k BETWEEN 30 AND 39 OR p = 100) AND v = 42
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_local_plan = 1, parallel_replicas_index_analysis_only_on_coordinator = 1,
    parallel_replicas_plan_based = 1, max_threads = 2, use_skip_indexes_on_data_read = 0, log_comment = '05401_plan_based_no_data_read';

SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['SelectedMarks'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = '05401_single';

-- A follower that got no work before the local replica finished is cancelled, and one that read nothing selects no
-- marks, so either passes: the timing decides how often a follower reads, not whether the check is correct.
-- Without the read-time skip-index evaluation a follower accounts the whole parts it planned as selected marks, so
-- those queries compare the rows read instead.
WITH (
    SELECT (ProfileEvents['SelectedMarks'], read_rows) FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = '05401_single'
) AS single
SELECT
    maxIf(ProfileEvents['SelectedMarks'], log_comment = '05401_query_based') <= single.1,
    maxIf(ProfileEvents['SelectedMarks'], log_comment = '05401_plan_based') <= single.1,
    maxIf(read_rows, log_comment = '05401_query_based_no_data_read') <= single.2,
    maxIf(read_rows, log_comment = '05401_plan_based_no_data_read') <= single.2
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND NOT is_initial_query
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query
            AND log_comment IN ('05401_query_based', '05401_plan_based', '05401_query_based_no_data_read', '05401_plan_based_no_data_read'));

DROP TABLE t_pr_follower_skip_idx;
