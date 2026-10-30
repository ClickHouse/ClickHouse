-- Tags: no-parallel-replicas: index analysis runs on the replicas, the initiator does not see it

SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 0;
SET optimize_use_projection_filtering = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET distributed_index_analysis = 0;

DROP TABLE IF EXISTS t_projection_candidates;
DROP TABLE IF EXISTS t_projection_candidates_reversed;
DROP TABLE IF EXISTS t_projection_filtering;

CREATE TABLE t_projection_candidates (k UInt64, a UInt64, b UInt64, PROJECTION p_a (SELECT * ORDER BY a), PROJECTION p_b (SELECT * ORDER BY b))
ENGINE = MergeTree
PARTITION BY intDiv(k, 32)
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- the same projections, `p_b` is analyzed first
CREATE TABLE t_projection_candidates_reversed (k UInt64, a UInt64, b UInt64, PROJECTION p_b (SELECT * ORDER BY b), PROJECTION p_a (SELECT * ORDER BY a))
ENGINE = MergeTree
PARTITION BY intDiv(k, 32)
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- `p_a` does not have `k`, so it only filters parts
CREATE TABLE t_projection_filtering (k UInt64, a UInt64, PROJECTION p_a (SELECT a ORDER BY a))
ENGINE = MergeTree
PARTITION BY intDiv(k, 32)
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- two parts of four granules each, `a = 1` only in the second part
INSERT INTO t_projection_candidates SELECT number, if(number < 32, 100, 1), number FROM numbers(64);
INSERT INTO t_projection_candidates_reversed SELECT number, if(number < 32, 100, 1), number FROM numbers(64);
INSERT INTO t_projection_filtering SELECT number, if(number < 32, 100, 1) FROM numbers(64);

-- `p_a` excludes the first part and is rejected, `p_b` excludes three granules of the second part
SELECT k FROM t_projection_candidates WHERE a = 1 AND b = 33 SETTINGS log_comment = 'projection_candidates_1';
-- `p_a` excludes the first part, both projections are rejected and the table reads the second part
SELECT count() FROM t_projection_candidates WHERE a = 1 SETTINGS log_comment = 'projection_candidates_2';
-- the table key excludes two granules of the first part, `p_a` excludes all four of them
SELECT count() FROM t_projection_candidates WHERE a = 1 AND k >= 24 SETTINGS log_comment = 'projection_candidates_3';
-- `p_b` keeps granules of both parts and is selected, then `p_a` excludes the first part: `p_b` still reads it
SELECT k FROM t_projection_candidates_reversed WHERE a = 1 AND b IN (5, 33) SETTINGS log_comment = 'projection_candidates_4';
-- `p_a` excludes the first part
SELECT count() FROM t_projection_filtering WHERE a = 1 SETTINGS optimize_use_projection_filtering = 1, log_comment = 'projection_candidates_5';
-- no projections
SELECT count() FROM t_projection_candidates WHERE a = 1 SETTINGS optimize_use_projections = 0, log_comment = 'projection_candidates_6';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), projections),
    ProfileEvents['GranulesDroppedByPartitionKey'],
    ProfileEvents['GranulesDroppedByPrimaryKey'],
    ProfileEvents['GranulesDroppedBySkipIndexes']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'projection_candidates_%'
ORDER BY log_comment;

DROP TABLE t_projection_candidates;
DROP TABLE t_projection_candidates_reversed;
DROP TABLE t_projection_filtering;
