-- Tags: no-parallel-replicas
-- no-parallel-replicas: index analysis runs on the replicas, the initiator does not see it

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
DROP TABLE IF EXISTS t_projection_index_offset;
DROP TABLE IF EXISTS t_projection_index_offset_chosen;
DROP TABLE IF EXISTS t_projection_index_offset_mixed;

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

-- `p` has `_part_offset`, so it filters the rows of the part that it keeps
CREATE TABLE t_projection_index_offset (k UInt64, a UInt64, b UInt64, PROJECTION p (SELECT a, b, _part_offset ORDER BY b) WITH SETTINGS (add_minmax_index_for_numeric_columns = 1))
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- `p_a` is selected and reads the part that `p` filters
CREATE TABLE t_projection_index_offset_chosen (k UInt64, a UInt64, b UInt64,
    PROJECTION p (SELECT a, b, _part_offset ORDER BY b) WITH SETTINGS (add_minmax_index_for_numeric_columns = 1),
    PROJECTION p_a (SELECT * ORDER BY a))
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- only the second part has `p_a`
CREATE TABLE t_projection_index_offset_mixed (k UInt64, a UInt64, b UInt64, PROJECTION p (SELECT a, b, _part_offset ORDER BY b) WITH SETTINGS (add_minmax_index_for_numeric_columns = 1))
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- two parts of four granules each, `a = 1` only in the second part
INSERT INTO t_projection_index SELECT number, if(number < 32, 100, 1), number FROM numbers(64);
INSERT INTO t_projection_index_filtering SELECT number, if(number < 32, 100, 1), number FROM numbers(64);
-- every granule of `t_projection_index_chosen` has `b` values from all over the range
INSERT INTO t_projection_index_chosen SELECT number, if(number < 32, 100, 1), 8 * (number % 8) + intDiv(number, 8) FROM numbers(64);
-- one part of eight granules
INSERT INTO t_projection_index_offset SELECT number, number, number FROM numbers(64);
INSERT INTO t_projection_index_offset_chosen SELECT number, number, number FROM numbers(64);
-- two parts of eight granules each
INSERT INTO t_projection_index_offset_mixed SELECT number, number, number FROM numbers(64);
ALTER TABLE t_projection_index_offset_mixed ADD PROJECTION p_a (SELECT * ORDER BY a);
INSERT INTO t_projection_index_offset_mixed SELECT number, number, number FROM numbers(64, 64);

-- the index of `p` excludes the first part, `p` is rejected and the table reads the second part
SELECT count() FROM t_projection_index WHERE a = 1 SETTINGS log_comment = 'projection_index_1';
-- the index of `p` excludes the first part
SELECT count() FROM t_projection_index_filtering WHERE a = 1 SETTINGS optimize_use_projection_filtering = 1, log_comment = 'projection_index_2';
-- `p_b` keeps one granule of each part and is selected, then the index of `p_k` excludes the first part: `p_b` still reads it
SELECT k FROM t_projection_index_chosen WHERE a = 1 AND b IN (57, 61) SETTINGS log_comment = 'projection_index_3';
-- no projections
SELECT count() FROM t_projection_index WHERE a = 1 SETTINGS optimize_use_projections = 0, log_comment = 'projection_index_4';
-- the index of `p` keeps one granule of the part, and the read uses the rows of `p` in it
SELECT k FROM t_projection_index_offset WHERE a = 5
SETTINGS optimize_use_projection_filtering = 1, min_table_rows_to_use_projection_index = 0, log_comment = 'projection_index_5';
-- `p_a` reads the part, so the index of `p` is not used
SELECT k FROM t_projection_index_offset_chosen WHERE a = 5
SETTINGS optimize_use_projection_filtering = 1, min_table_rows_to_use_projection_index = 0, log_comment = 'projection_index_6';
-- `p_a` reads the second part, the index of `p` keeps one granule of the first part: the optimization analyzes `p` again for it
SELECT sum(k) FROM t_projection_index_offset_mixed WHERE a = 5 OR a = 69
SETTINGS optimize_use_projection_filtering = 1, min_table_rows_to_use_projection_index = 0, log_comment = 'projection_index_7';

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
DROP TABLE t_projection_index_offset;
DROP TABLE t_projection_index_offset_chosen;
DROP TABLE t_projection_index_offset_mixed;
