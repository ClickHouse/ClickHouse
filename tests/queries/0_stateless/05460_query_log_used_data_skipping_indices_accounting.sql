-- Tags: no-parallel-replicas
-- no-parallel-replicas: index analysis runs on the replicas, the initiator does not see it

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_statistics_for_part_pruning = 0;
SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET distributed_index_analysis = 0;

DROP TABLE IF EXISTS t_skip_index_accounting;

CREATE TABLE t_skip_index_accounting
(
    k UInt64,
    a UInt64,
    b UInt64,
    c UInt64,
    INDEX idx_a a TYPE minmax GRANULARITY 1,
    INDEX idx_b b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY intDiv(k, 32)
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0,
    distributed_index_analysis_min_parts_to_activate = 0, distributed_index_analysis_min_indexes_bytes_to_activate = 0;

-- two parts, four granules of eight rows each
INSERT INTO t_skip_index_accounting SELECT number, number, number, number FROM numbers(64);

-- the query condition cache drops seven granules before the primary key on the second run
SELECT count() FROM t_skip_index_accounting WHERE a = 5 SETTINGS use_query_condition_cache = 1, log_comment = 'skip_index_accounting_1';
SELECT count() FROM t_skip_index_accounting WHERE a = 5 SETTINGS use_query_condition_cache = 1, log_comment = 'skip_index_accounting_2';
-- per-part statistics do not change the totals
SELECT count() FROM t_skip_index_accounting WHERE a = 5 SETTINGS use_query_condition_cache = 0, per_part_index_stats = 1, log_comment = 'skip_index_accounting_3';
-- distributed index analysis on the local replica only
SELECT count() FROM t_skip_index_accounting WHERE a = 5
SETTINGS use_query_condition_cache = 0, distributed_index_analysis = 1, distributed_index_analysis_for_non_shared_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_shard_localhost', log_comment = 'skip_index_accounting_4';

-- an index that is not materialized in any part is not used
ALTER TABLE t_skip_index_accounting ADD INDEX idx_c c TYPE minmax GRANULARITY 1;
SELECT count() FROM t_skip_index_accounting WHERE c = 5 SETTINGS use_query_condition_cache = 0, log_comment = 'skip_index_accounting_5';

-- an index on a column with a pending update is not used
SYSTEM STOP MERGES t_skip_index_accounting;
ALTER TABLE t_skip_index_accounting UPDATE b = b + 1 WHERE 1 SETTINGS mutations_sync = 0;
SELECT count() FROM t_skip_index_accounting WHERE b = 5 SETTINGS use_query_condition_cache = 0, apply_mutations_on_fly = 1, log_comment = 'skip_index_accounting_6';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), used_data_skipping_indices),
    ProfileEvents['GranulesDroppedByPartitionKey'],
    ProfileEvents['GranulesDroppedByPrimaryKey'],
    ProfileEvents['GranulesDroppedBySkipIndexes']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'skip_index_accounting_%'
ORDER BY log_comment;

DROP TABLE t_skip_index_accounting;
