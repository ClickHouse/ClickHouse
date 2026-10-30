-- Tags: no-parallel-replicas: index analysis runs on the replicas, the initiator does not see it

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET optimize_trivial_count_query = 0;
SET optimize_use_projection_filtering = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET distributed_index_analysis = 0;

DROP TABLE IF EXISTS t_skip_index_projection;
DROP TABLE IF EXISTS t_skip_index_aggregate_projection;

CREATE TABLE t_skip_index_projection
(
    k UInt64,
    a UInt64,
    INDEX idx_a a TYPE minmax GRANULARITY 2,
    PROJECTION p_a (SELECT * ORDER BY a)
)
ENGINE = MergeTree
PARTITION BY intDiv(k, 32)
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

CREATE TABLE t_skip_index_aggregate_projection
(
    k UInt64,
    a UInt64,
    INDEX idx_a a TYPE minmax GRANULARITY 2,
    PROJECTION p_count (SELECT a, count() GROUP BY a)
)
ENGINE = MergeTree
PARTITION BY intDiv(k, 32)
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- two parts, four granules of eight rows each; idx_a excludes the second part and two granules of the first
INSERT INTO t_skip_index_projection SELECT number, number FROM numbers(64);
INSERT INTO t_skip_index_aggregate_projection SELECT number, number FROM numbers(64);

-- the projection reads the first part, so only the exclusion of the second part by idx_a counts
SELECT k FROM t_skip_index_projection WHERE a = 5 SETTINGS optimize_use_projections = 1, log_comment = 'skip_index_projection_1';
SELECT k FROM t_skip_index_projection WHERE a = 5 SETTINGS optimize_use_projections = 0, log_comment = 'skip_index_projection_2';
SELECT k FROM t_skip_index_projection WHERE a = 5 SETTINGS optimize_use_projections = 1, ignore_data_skipping_indices = 'idx_a', log_comment = 'skip_index_projection_3';
-- the same for an aggregate projection
SELECT count() FROM t_skip_index_aggregate_projection WHERE a = 5 SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 0, log_comment = 'skip_index_projection_4';
SELECT count() FROM t_skip_index_aggregate_projection WHERE a = 5 SETTINGS optimize_use_projections = 0, optimize_use_implicit_projections = 0, log_comment = 'skip_index_projection_5';
-- the implicit count projection excludes the first partition
SELECT count() FROM t_skip_index_projection WHERE intDiv(k, 32) = 1 SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, log_comment = 'skip_index_projection_6';
SELECT count() FROM t_skip_index_projection WHERE intDiv(k, 32) = 1 SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 0, log_comment = 'skip_index_projection_7';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), projections),
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), used_data_skipping_indices),
    ProfileEvents['GranulesDroppedByPartitionKey'],
    ProfileEvents['GranulesDroppedByPrimaryKey'],
    ProfileEvents['GranulesDroppedBySkipIndexes']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'skip_index_projection_%'
ORDER BY log_comment;

DROP TABLE t_skip_index_projection;
DROP TABLE t_skip_index_aggregate_projection;
