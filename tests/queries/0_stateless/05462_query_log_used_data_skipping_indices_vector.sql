-- Tags: no-fasttest, no-parallel-replicas
-- no-fasttest: needs the vector similarity index
-- no-parallel-replicas: index analysis runs on the replicas, the initiator does not see it

SET use_query_condition_cache = 0;
SET vector_search_filter_strategy = 'postfilter';
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET distributed_index_analysis = 0;

DROP TABLE IF EXISTS t_skip_index_vector;

CREATE TABLE t_skip_index_vector
(
    k UInt64,
    vec Array(Float32),
    INDEX idx_vec vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 10000
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 8, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- one part of eight granules
INSERT INTO t_skip_index_vector SELECT number, [toFloat32(number), 0] FROM numbers(64);

SELECT k FROM t_skip_index_vector ORDER BY L2Distance(vec, [1.0, 0.0]) LIMIT 1 SETTINGS log_comment = 'skip_index_vector_1';
-- the primary key excludes some granules of the part, so the vector index is not used
SELECT k FROM t_skip_index_vector WHERE k < 20 ORDER BY L2Distance(vec, [1.0, 0.0]) LIMIT 1 SETTINGS log_comment = 'skip_index_vector_2';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), used_data_skipping_indices),
    ProfileEvents['USearchSearchCount'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'skip_index_vector_%'
ORDER BY log_comment;

DROP TABLE t_skip_index_vector;
