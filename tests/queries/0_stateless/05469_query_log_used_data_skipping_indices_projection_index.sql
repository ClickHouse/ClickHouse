-- Tags: no-parallel-replicas: index analysis runs on the replicas, the initiator does not see it

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 0;
SET optimize_use_projection_filtering = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET distributed_index_analysis = 0;

DROP TABLE IF EXISTS t_projection_index;
DROP TABLE IF EXISTS t_projection_index_filtering;
DROP TABLE IF EXISTS t_projection_index_chosen;

-- only the projection `p` has an index on `a`
CREATE TABLE t_projection_index (k UInt64, a UInt64, b UInt64, PROJECTION p (SELECT * ORDER BY b) WITH SETTINGS (add_minmax_index_for_numeric_columns = 1))
ENGINE = MergeTree
PARTITION BY intDiv(k, 32)
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- `p` does not have `k`, so it only filters parts
CREATE TABLE t_projection_index_filtering (k UInt64, a UInt64, b UInt64, PROJECTION p (SELECT a, b ORDER BY b) WITH SETTINGS (add_minmax_index_for_numeric_columns = 1))
ENGINE = MergeTree
PARTITION BY intDiv(k, 32)
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- `p_b` is analyzed first, `p_k` has an index on `a`
CREATE TABLE t_projection_index_chosen (k UInt64, a UInt64, b UInt64,
    PROJECTION p_b (SELECT * ORDER BY b),
    PROJECTION p_k (SELECT * ORDER BY k) WITH SETTINGS (add_minmax_index_for_numeric_columns = 1))
ENGINE = MergeTree
PARTITION BY intDiv(k, 32)
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- two parts of four granules each, `a = 1` only in the second part
INSERT INTO t_projection_index SELECT number, if(number < 32, 100, 1), number FROM numbers(64);
INSERT INTO t_projection_index_filtering SELECT number, if(number < 32, 100, 1), number FROM numbers(64);
-- every granule of `t_projection_index_chosen` has `b` values from all over the range
INSERT INTO t_projection_index_chosen SELECT number, if(number < 32, 100, 1), 8 * (number % 8) + intDiv(number, 8) FROM numbers(64);

-- the index of `p` excludes the first part, `p` is rejected and the table reads the second part
SELECT count() FROM t_projection_index WHERE a = 1 SETTINGS log_comment = 'projection_index_1';
-- the index of `p` excludes the first part
SELECT count() FROM t_projection_index_filtering WHERE a = 1 SETTINGS optimize_use_projection_filtering = 1, log_comment = 'projection_index_2';
-- `p_b` keeps one granule of each part and is selected, then the index of `p_k` excludes the first part: `p_b` still reads it
SELECT k FROM t_projection_index_chosen WHERE a = 1 AND b IN (57, 61) SETTINGS log_comment = 'projection_index_3';
-- no projections
SELECT count() FROM t_projection_index WHERE a = 1 SETTINGS optimize_use_projections = 0, log_comment = 'projection_index_4';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    read_rows,
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), projections),
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), used_data_skipping_indices),
    ProfileEvents['GranulesDroppedByPrimaryKey'],
    ProfileEvents['GranulesDroppedBySkipIndexes']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'projection_index_%'
ORDER BY log_comment;

DROP TABLE t_projection_index;
DROP TABLE t_projection_index_filtering;
DROP TABLE t_projection_index_chosen;
