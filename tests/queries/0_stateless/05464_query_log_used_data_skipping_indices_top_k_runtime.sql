-- Tags: no-parallel-replicas: index analysis runs on the replicas, the initiator does not see it

SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET distributed_index_analysis = 0;

DROP TABLE IF EXISTS t_skip_index_top_k_runtime;

CREATE TABLE t_skip_index_top_k_runtime
(
    k UInt64,
    a UInt64,
    b UInt64,
    INDEX idx_a a TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_skip_index_top_k_runtime SELECT number, number, number % 2 FROM numbers(10000);

-- the filter does not use idx_a, the dynamic top-K filter reads it when the part is read
SELECT k FROM t_skip_index_top_k_runtime WHERE b > 0 ORDER BY a LIMIT 1
SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1, use_skip_indexes_on_data_read = 1, max_threads = 1, max_block_size = 8,
    log_comment = 'skip_index_top_k_runtime_1';
SELECT k FROM t_skip_index_top_k_runtime WHERE b > 0 ORDER BY a LIMIT 1
SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 1, use_skip_indexes_on_data_read = 1, max_threads = 1, max_block_size = 8,
    log_comment = 'skip_index_top_k_runtime_2';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), used_data_skipping_indices),
    read_rows < 10000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'skip_index_top_k_runtime_%'
ORDER BY log_comment;

DROP TABLE t_skip_index_top_k_runtime;
