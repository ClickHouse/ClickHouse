-- Tags: no-parallel-replicas
-- no-parallel-replicas: index analysis runs on the replicas, the initiator does not see it

SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET optimize_trivial_count_query = 0;
SET optimize_use_projections = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET distributed_index_analysis = 0;
-- a throwing `max_rows_to_read` turns off skip indexes on data read, the test profile sets it
SET max_rows_to_read = 0;

DROP TABLE IF EXISTS t_skip_index_disjunction;

CREATE TABLE t_skip_index_disjunction
(
    k UInt64,
    a UInt64,
    b UInt64,
    c UInt64,
    INDEX idx_a a TYPE minmax GRANULARITY 1,
    INDEX idx_b b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

SYSTEM STOP MERGES t_skip_index_disjunction;
-- two parts of eight granules
INSERT INTO t_skip_index_disjunction SELECT number, number, number, number FROM numbers(64);
INSERT INTO t_skip_index_disjunction SELECT number, number, number, number FROM numbers(64, 64);

-- neither index excludes a granule on its own, the merge of their results excludes seven granules in each part
SELECT k FROM t_skip_index_disjunction WHERE a = 5 OR b = 70 ORDER BY k SETTINGS use_skip_indexes_for_disjunctions = 1, use_skip_indexes_on_data_read = 0, log_comment = 'skip_index_disjunction_1';
-- a column without an index keeps every granule, so no index is used
SELECT k FROM t_skip_index_disjunction WHERE a = 5 OR c = 70 ORDER BY k SETTINGS use_skip_indexes_for_disjunctions = 1, use_skip_indexes_on_data_read = 0, log_comment = 'skip_index_disjunction_2';
-- without the disjunction analysis each index alone excludes nothing
SELECT k FROM t_skip_index_disjunction WHERE a = 5 OR b = 70 ORDER BY k SETTINGS use_skip_indexes_for_disjunctions = 0, use_skip_indexes_on_data_read = 0, log_comment = 'skip_index_disjunction_3';
-- the same merge at read time
SELECT k FROM t_skip_index_disjunction WHERE a = 5 OR b = 70 ORDER BY k SETTINGS use_skip_indexes_for_disjunctions = 1, use_skip_indexes_on_data_read = 1, log_comment = 'skip_index_disjunction_4';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), used_data_skipping_indices),
    ProfileEvents['GranulesDroppedBySkipIndexes'],
    ProfileEvents['SelectedMarks']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'skip_index_disjunction_%'
ORDER BY log_comment;

DROP TABLE t_skip_index_disjunction;
