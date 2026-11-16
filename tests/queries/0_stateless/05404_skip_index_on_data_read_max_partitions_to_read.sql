-- `max_partitions_to_read` is checked before the read, so it must count only the parts left after skip indexes,
-- even with `use_skip_indexes_on_data_read = 1`.

SET use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
-- A row limit with `read_overflow_mode = 'throw'` already disables read-time skip indexes.
SET max_rows_to_read = 0, max_rows_to_read_leaf = 0;
-- Entries written by an earlier query would drop the parts without the skip index.
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_skip_idx_partitions;

CREATE TABLE t_skip_idx_partitions
(
    d Date,
    k UInt64,
    s String,
    INDEX idx_s s TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree PARTITION BY d ORDER BY k
SETTINGS index_granularity = 128, max_partitions_to_read = 2;

-- 5 partitions; 'needle' exists only in the last one, every 'hay-N' in all of them.
INSERT INTO t_skip_idx_partitions SELECT toDate('2026-01-01') + number % 5, number, if(number % 5 = 4 AND number % 100 = 4, 'needle', 'hay-' || toString(number % 7)) FROM numbers(10000);

SELECT count() FROM t_skip_idx_partitions WHERE s = 'needle';
SELECT count() FROM t_skip_idx_partitions WHERE s = 'needle' SETTINGS max_partitions_to_read = 0;
SELECT count() FROM t_skip_idx_partitions WHERE s = 'hay-1'; -- { serverError TOO_MANY_PARTITIONS }

-- Parallel replicas with index analysis only on the coordinator.
SELECT sum(k) FROM t_skip_idx_partitions WHERE s = 'needle'
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_local_plan = 1, parallel_replicas_index_analysis_only_on_coordinator = 1, parallel_replicas_support_projection = 0,
    log_comment = '05404_pr';

-- The initiator tolerates a follower that fails the limit, so check the followers' queries.
SYSTEM FLUSH LOGS query_log;
SELECT countIf(errorCodeToName(exception_code) = 'TOO_MANY_PARTITIONS') FROM system.query_log
WHERE event_date >= yesterday() AND type != 'QueryStart' AND initial_query_id IN (
    SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND log_comment = '05404_pr' AND is_initial_query);

-- Without a local plan on the initiator the followers analyze and check the limit themselves.
SELECT sum(k) FROM t_skip_idx_partitions WHERE s = 'needle'
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_local_plan = 1, parallel_replicas_prefer_local_replica = 0,
    parallel_replicas_index_analysis_only_on_coordinator = 1, parallel_replicas_support_projection = 0;
SELECT sum(k) FROM t_skip_idx_partitions WHERE s = 'hay-1'
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_local_plan = 1, parallel_replicas_prefer_local_replica = 0,
    parallel_replicas_index_analysis_only_on_coordinator = 1, parallel_replicas_support_projection = 0; -- { serverError TOO_MANY_PARTITIONS }

-- The query-level setting takes precedence over the table one.
ALTER TABLE t_skip_idx_partitions MODIFY SETTING max_partitions_to_read = 0;
SELECT count() FROM t_skip_idx_partitions WHERE s = 'needle' SETTINGS max_partitions_to_read = 1;
SELECT count() FROM t_skip_idx_partitions WHERE s = 'hay-1' SETTINGS max_partitions_to_read = 1; -- { serverError TOO_MANY_PARTITIONS }

DROP TABLE t_skip_idx_partitions;
