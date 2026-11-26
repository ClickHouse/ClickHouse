-- `automatic_parallel_replicas_max_counted_ranges` is charged by the ranges of compressed blocks a query keeps, not
-- only by the files it reads: a budget that admits both files of the read is exhausted by their disjoint ranges, the
-- query still completes, and the next run declines parallel replicas because its input was not measured. With no
-- limit, the statistics collected again measure the input, and the next run is decided by the cost model.

DROP TABLE IF EXISTS t_autopr_ranges_capacity;

-- One compressed block per granule, and the skip index keeps every other granule, so each of the two files read
-- yields one disjoint range per selected granule.
CREATE TABLE t_autopr_ranges_capacity
(
    key UInt64,
    value String,
    INDEX i_half key % 256 TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_compress_block_size = 1,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_autopr_ranges_capacity SELECT number, toString(number) FROM numbers(100000);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET automatic_parallel_replicas_min_bytes_per_replica = 0;
-- The runs must select the same granules, see `05332_autopr_min_bytes_per_replica_events`.
SET use_query_condition_cache = 0;
-- The filter is written as `PREWHERE`: under `parallel_replicas_plan_based = 1`, a filter that `optimize_move_to_prewhere`
-- moves makes the plans not comparable, and no statistics are collected.

-- Two files take 16 units; the hundreds of ranges they read take the rest.
SELECT key % 10 AS k, max(value) FROM t_autopr_ranges_capacity PREWHERE key % 256 < 128 GROUP BY k FORMAT Null
SETTINGS automatic_parallel_replicas_max_counted_ranges = 100, log_comment = 'autopr_ranges_capacity_1_collect';
SELECT key % 10 AS k, max(value) FROM t_autopr_ranges_capacity PREWHERE key % 256 < 128 GROUP BY k FORMAT Null
SETTINGS automatic_parallel_replicas_max_counted_ranges = 100, log_comment = 'autopr_ranges_capacity_2_rejected';

-- The table grows past the drift ratio, so the next run collects the statistics again, now without a limit.
INSERT INTO t_autopr_ranges_capacity SELECT number + 100000, toString(number) FROM numbers(300000);
SELECT key % 10 AS k, max(value) FROM t_autopr_ranges_capacity PREWHERE key % 256 < 128 GROUP BY k FORMAT Null
SETTINGS automatic_parallel_replicas_max_counted_ranges = 0, log_comment = 'autopr_ranges_capacity_3_recollect';
SELECT key % 10 AS k, max(value) FROM t_autopr_ranges_capacity PREWHERE key % 256 < 128 GROUP BY k FORMAT Null
SETTINGS automatic_parallel_replicas_max_counted_ranges = 0, log_comment = 'autopr_ranges_capacity_4_evaluated';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AutoParallelReplicasNoStatistics'] AS collected,
       ProfileEvents['AutoParallelReplicasStatisticsDrifted'] AS recollected,
       ProfileEvents['AutoParallelReplicasRejectedByUnmeasuredInput'] AS rejected,
       ProfileEvents['AutoParallelReplicasCostModelEvaluated'] AS evaluated
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND startsWith(log_comment, 'autopr_ranges_capacity_')
    AND (type = 'QueryFinish') AND is_initial_query
ORDER BY event_time_microseconds;

DROP TABLE t_autopr_ranges_capacity;
