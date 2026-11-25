-- Tags: no-parallel-replicas
-- The threads that wait for the projection index of a part help read it when its projection parts have enough marks.

SET optimize_use_projections = 1, optimize_use_projection_filtering = 1, use_query_condition_cache = 0, use_indexes_refiner_in_read_pools = 0, max_threads_min_free_memory_per_thread = 0;
SET max_projection_rows_to_use_projection_index = 1000000000, min_table_rows_to_use_projection_index = 0;
-- A task reads at least 4096 rows: 32 marks of 128 rows.
SET merge_tree_min_rows_for_concurrent_read = 4096, merge_tree_min_bytes_for_concurrent_read = 0;

DROP TABLE IF EXISTS t_projection_index_parallel;
DROP TABLE IF EXISTS t_projection_index_parallel_parts;

CREATE TABLE t_projection_index_parallel
(
    a UInt32,
    b UInt32,
    c UInt64,
    PROJECTION pa INDEX a TYPE basic,
    PROJECTION pb INDEX b TYPE basic
)
ENGINE = MergeTree
ORDER BY c
SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- One insert block, so one part without a merge.
INSERT INTO t_projection_index_parallel SELECT number % 50, intDiv(number, 50) % 50, number FROM numbers(2000000)
SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 4000000, min_insert_block_size_bytes = 0, max_insert_block_size = 4000000;

SELECT 'one part';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_projection_index_parallel' AND active;
SELECT count(), sum(c) FROM t_projection_index_parallel WHERE a = 3 AND b = 5 SETTINGS max_threads = 8, log_comment = 'projection_index_parallel_8';
SELECT count(), sum(c) FROM t_projection_index_parallel WHERE a = 3 AND b = 5 SETTINGS max_threads = 1, log_comment = 'projection_index_parallel_1';
SELECT count(), sum(c) FROM t_projection_index_parallel WHERE a = 3 AND b = 5 SETTINGS optimize_use_projection_filtering = 0;

SELECT 'primary key range';
SELECT count(), sum(c) FROM t_projection_index_parallel WHERE a = 3 AND b = 5 AND c < 1000000 SETTINGS max_threads = 8;
SELECT count(), sum(c) FROM t_projection_index_parallel WHERE a = 3 AND b = 5 AND c < 1000000 SETTINGS max_threads = 1;
SELECT count(), sum(c) FROM t_projection_index_parallel WHERE a = 3 AND b = 5 AND c < 1000000 SETTINGS optimize_use_projection_filtering = 0;

SELECT 'IN and range';
SELECT count(), sum(c) FROM t_projection_index_parallel WHERE a IN (1, 4, 40) AND b BETWEEN 2 AND 6 SETTINGS max_threads = 8;
SELECT count(), sum(c) FROM t_projection_index_parallel WHERE a IN (1, 4, 40) AND b BETWEEN 2 AND 6 SETTINGS max_threads = 1;
SELECT count(), sum(c) FROM t_projection_index_parallel WHERE a IN (1, 4, 40) AND b BETWEEN 2 AND 6 SETTINGS optimize_use_projection_filtering = 0;

SELECT 'max_threads_for_indexes';
SELECT count(), sum(c) FROM t_projection_index_parallel WHERE a = 3 AND b = 5 SETTINGS max_threads = 8, max_threads_for_indexes = 4;

SELECT 'indexes refiner';
SELECT count(), sum(c) FROM t_projection_index_parallel WHERE a = 3 AND b = 5 SETTINGS max_threads = 8, use_indexes_refiner_in_read_pools = 1;

-- Three parts, and the projection `pb` has granules of its own size.
CREATE TABLE t_projection_index_parallel_parts
(
    a UInt32,
    b UInt32,
    c UInt64,
    PROJECTION pa INDEX a TYPE basic,
    PROJECTION pb INDEX b TYPE basic WITH SETTINGS (index_granularity = 64)
)
ENGINE = MergeTree
ORDER BY c
SETTINGS index_granularity = 128, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_projection_index_parallel_parts;
INSERT INTO t_projection_index_parallel_parts SELECT number % 50, intDiv(number, 50) % 50, number FROM numbers(0, 700000);
INSERT INTO t_projection_index_parallel_parts SELECT number % 50, intDiv(number, 50) % 50, number FROM numbers(700000, 700000);
INSERT INTO t_projection_index_parallel_parts SELECT number % 50, intDiv(number, 50) % 50, number FROM numbers(1400000, 600000);

SELECT 'three parts';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_projection_index_parallel_parts' AND active;
SELECT count(), sum(c) FROM t_projection_index_parallel_parts WHERE a = 3 AND b = 5 SETTINGS max_threads = 8;
SELECT count(), sum(c) FROM t_projection_index_parallel_parts WHERE a = 3 AND b = 5 SETTINGS max_threads = 1;
SELECT count(), sum(c) FROM t_projection_index_parallel_parts WHERE a = 3 AND b = 5 SETTINGS optimize_use_projection_filtering = 0;

-- The projection index skipped most of the 2M rows with both thread counts.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, read_rows < 500000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'projection_index_parallel_%'
ORDER BY log_comment;

DROP TABLE t_projection_index_parallel;
DROP TABLE t_projection_index_parallel_parts;
