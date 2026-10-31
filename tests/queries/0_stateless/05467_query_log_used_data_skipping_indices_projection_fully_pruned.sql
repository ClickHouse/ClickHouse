-- Tags: no-parallel-replicas
-- no-parallel-replicas: index analysis runs on the replicas, the initiator does not see it

SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET optimize_trivial_count_query = 0;
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 0;
SET optimize_use_projection_filtering = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET distributed_index_analysis = 0;

DROP TABLE IF EXISTS t_pruned_projection;
DROP TABLE IF EXISTS t_pruned_aggregate_projection;
DROP TABLE IF EXISTS t_pruned_projection_mixed;

CREATE TABLE t_pruned_projection (k UInt64, a UInt64, PROJECTION p_a (SELECT * ORDER BY a))
ENGINE = MergeTree
PARTITION BY intDiv(k, 32)
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

CREATE TABLE t_pruned_aggregate_projection (k UInt64, a UInt64, PROJECTION p_count (SELECT a, k, count() GROUP BY a, k))
ENGINE = MergeTree
PARTITION BY intDiv(k, 32)
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

CREATE TABLE t_pruned_projection_mixed (k UInt64, a UInt64)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- two parts of four granules each, in both the tables and the projections
INSERT INTO t_pruned_projection SELECT number, number + 100 FROM numbers(64);
INSERT INTO t_pruned_aggregate_projection SELECT number, number + 100 FROM numbers(64);
-- only the second part has the projection
INSERT INTO t_pruned_projection_mixed SELECT number, number + 100 FROM numbers(32);
ALTER TABLE t_pruned_projection_mixed ADD PROJECTION p_a (SELECT * ORDER BY a);
INSERT INTO t_pruned_projection_mixed SELECT number, number + 100 FROM numbers(32, 32);

-- the projection key excludes all eight granules
SELECT k FROM t_pruned_projection WHERE a = 1 SETTINGS log_comment = 'pruned_projection_1';
SELECT k FROM t_pruned_projection WHERE a = 1 SETTINGS optimize_use_projections = 0, log_comment = 'pruned_projection_2';
-- the partition key excludes the second part; the projection excludes the four granules of the first, in which the table key excludes three
SELECT k FROM t_pruned_projection WHERE a = 1 AND k < 8 SETTINGS log_comment = 'pruned_projection_3';
-- the same for an aggregate projection
SELECT count() FROM t_pruned_aggregate_projection WHERE a = 1 SETTINGS log_comment = 'pruned_projection_4';
SELECT count() FROM t_pruned_aggregate_projection WHERE a = 1 AND k < 8 SETTINGS log_comment = 'pruned_projection_5';
-- the table reads the first part, the projection excludes the four granules of the second; ORDER BY a selects it at equal marks
SELECT k FROM t_pruned_projection_mixed WHERE a = 1 ORDER BY a SETTINGS optimize_read_in_order = 1, log_comment = 'pruned_projection_6';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), projections),
    ProfileEvents['GranulesDroppedByPartitionKey'],
    ProfileEvents['GranulesDroppedByPrimaryKey'],
    ProfileEvents['GranulesDroppedBySkipIndexes']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'pruned_projection_%'
ORDER BY log_comment;

DROP TABLE t_pruned_projection;
DROP TABLE t_pruned_aggregate_projection;
DROP TABLE t_pruned_projection_mixed;
