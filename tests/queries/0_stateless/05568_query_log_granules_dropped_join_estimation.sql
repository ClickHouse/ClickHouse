-- Tags: no-parallel-replicas
-- no-parallel-replicas: index analysis runs on the replicas, the initiator does not see it

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET optimize_trivial_count_query = 0;
SET optimize_use_projections = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET distributed_index_analysis = 0;
SET query_plan_optimize_join_order_limit = 10;

DROP TABLE IF EXISTS t_granules_dropped_estimation;

CREATE TABLE t_granules_dropped_estimation
(
    k UInt64,
    a UInt64,
    INDEX idx_a a TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY intDiv(k, 32)
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- two partitions of four granules
INSERT INTO t_granules_dropped_estimation SELECT number, number FROM numbers(64);

-- with a throwing read limit the join order estimate analyzes the indexes and discards the result, the read analyzes them again
-- the partition key excludes four granules, the primary key three
SELECT sum(t.k) FROM t_granules_dropped_estimation AS t CROSS JOIN numbers(1) AS n WHERE t.k = 5 SETTINGS max_rows_to_read = 100000, log_comment = 'granules_dropped_estimation_1';
-- the skip index excludes seven granules
SELECT sum(t.k) FROM t_granules_dropped_estimation AS t CROSS JOIN numbers(1) AS n WHERE t.a = 5 SETTINGS max_rows_to_read = 100000, log_comment = 'granules_dropped_estimation_2';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), used_data_skipping_indices),
    ProfileEvents['GranulesDroppedByPartitionKey'],
    ProfileEvents['GranulesDroppedByPrimaryKey'],
    ProfileEvents['GranulesDroppedBySkipIndexes']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'granules_dropped_estimation_%'
ORDER BY log_comment;

DROP TABLE t_granules_dropped_estimation;
