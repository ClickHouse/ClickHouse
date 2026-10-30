-- Tags: no-parallel-replicas: index analysis runs on the replicas, the initiator does not see it

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS t_skip_index_usage;

CREATE TABLE t_skip_index_usage
(
    k UInt64,
    a UInt64,
    b UInt64,
    INDEX idx_a a TYPE minmax GRANULARITY 1,
    INDEX idx_b b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY intDiv(k, 32)
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- two parts, four granules of eight rows each
INSERT INTO t_skip_index_usage SELECT number, number, number FROM numbers(64);

-- one skip index drops seven of eight granules
SELECT count() FROM t_skip_index_usage WHERE a = 5 SETTINGS log_comment = 'skip_index_usage_1';
-- the same filter with the index ignored: nothing is used
SELECT count() FROM t_skip_index_usage WHERE a = 5 SETTINGS ignore_data_skipping_indices = 'idx_a', log_comment = 'skip_index_usage_2';
-- partition key drops one part, primary key drops three of the remaining four granules
SELECT count() FROM t_skip_index_usage WHERE k = 5 SETTINGS log_comment = 'skip_index_usage_3';
-- two skip indexes: idx_a keeps one granule, idx_b drops it
SELECT count() FROM t_skip_index_usage WHERE a = 5 AND b = 60 SETTINGS log_comment = 'skip_index_usage_4';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), used_data_skipping_indices),
    ProfileEvents['GranulesDroppedByPartitionKey'],
    ProfileEvents['GranulesDroppedByPrimaryKey'],
    ProfileEvents['GranulesDroppedBySkipIndexes']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'skip_index_usage_%'
ORDER BY log_comment;

DROP TABLE t_skip_index_usage;
