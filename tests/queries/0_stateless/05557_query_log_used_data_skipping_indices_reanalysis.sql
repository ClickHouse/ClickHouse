-- Tags: no-parallel-replicas
-- no-parallel-replicas: index analysis runs on the replicas, the initiator does not see it

-- join order estimation analyzes the read, then the exact count optimization analyzes its pruned ranges again: keep the drops of both

SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET distributed_index_analysis = 0;
SET max_rows_to_read = 0;
SET optimize_move_to_prewhere = 0;
SET use_skip_indexes_on_data_read = 0;
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;

DROP TABLE IF EXISTS t_reanalysis_skip;
DROP TABLE IF EXISTS t_reanalysis_key;

CREATE TABLE t_reanalysis_skip (k UInt64, a UInt64)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

SYSTEM STOP MERGES t_reanalysis_skip;

-- the index is materialized only in the second part of eight granules, which it excludes entirely
INSERT INTO t_reanalysis_skip SELECT number, number FROM numbers(64);
ALTER TABLE t_reanalysis_skip ADD INDEX idx_a a TYPE minmax GRANULARITY 1;
INSERT INTO t_reanalysis_skip SELECT number + 64, number + 64 FROM numbers(64);

-- two partitions of four granules
CREATE TABLE t_reanalysis_key (k UInt64, a UInt64, INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree
PARTITION BY intDiv(k, 32)
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_reanalysis_key SELECT number, number FROM numbers(64);

SELECT c FROM (SELECT count() AS c FROM t_reanalysis_skip WHERE a = 5) AS counts CROSS JOIN numbers(1) AS n
SETTINGS query_plan_optimize_join_order_limit = 0, log_comment = 'reanalysis_1_off';
SELECT c FROM (SELECT count() AS c FROM t_reanalysis_skip WHERE a = 5) AS counts CROSS JOIN numbers(1) AS n
SETTINGS query_plan_optimize_join_order_limit = 10, log_comment = 'reanalysis_1_on';
SELECT c FROM (SELECT count() AS c FROM t_reanalysis_key WHERE k = 5) AS counts CROSS JOIN numbers(1) AS n
SETTINGS query_plan_optimize_join_order_limit = 0, log_comment = 'reanalysis_2_off';
SELECT c FROM (SELECT count() AS c FROM t_reanalysis_key WHERE k = 5) AS counts CROSS JOIN numbers(1) AS n
SETTINGS query_plan_optimize_join_order_limit = 10, log_comment = 'reanalysis_2_on';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['GranulesDroppedByPartitionKey'],
    ProfileEvents['GranulesDroppedByPrimaryKey'],
    ProfileEvents['GranulesDroppedBySkipIndexes'],
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), used_data_skipping_indices)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'reanalysis_%'
ORDER BY log_comment;

DROP TABLE t_reanalysis_skip;
DROP TABLE t_reanalysis_key;
