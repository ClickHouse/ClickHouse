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
SET max_rows_to_read = 0;

DROP TABLE IF EXISTS t_chosen_projection;

CREATE TABLE t_chosen_projection (k UInt64, a UInt64, INDEX idx_a a TYPE minmax GRANULARITY 2, PROJECTION p_a (SELECT * ORDER BY a))
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- one part of eight granules
INSERT INTO t_chosen_projection SELECT number, number FROM numbers(64);

-- `p_a` reads the only part, so the analysis of the table that applied `idx_a` to it is not used
SELECT k FROM t_chosen_projection WHERE a = 5 SETTINGS log_comment = 'chosen_projection_1';
-- no projections
SELECT k FROM t_chosen_projection WHERE a = 5 SETTINGS optimize_use_projections = 0, log_comment = 'chosen_projection_2';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    read_rows,
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), projections),
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), used_data_skipping_indices),
    ProfileEvents['GranulesDroppedByPrimaryKey'],
    ProfileEvents['GranulesDroppedBySkipIndexes']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'chosen_projection_%'
ORDER BY log_comment;

DROP TABLE t_chosen_projection;
