-- Tags: no-parallel-replicas, no-object-storage
-- The task size is lowered only for a local read from the plain read pool.

-- `merge_tree_read_end_tasks_per_stream` makes the tasks at the end of a `MergeTree` read 1/N of the share of a
-- stream, and 0 turns this off (05814 checks the default of 32). A block never spans two tasks, so with
-- `max_block_size` above the rows of a task every block is one task, and `blockSize` shows the task size. The
-- table has 768 granules of 128 rows in one part, so 2 streams have 384 marks each, which is 16 default tasks of
-- 24 marks. The end of the read is the last 2 * 24 marks.

DROP TABLE IF EXISTS t_read_end_tasks;
CREATE TABLE t_read_end_tasks (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_read_end_tasks SELECT number FROM numbers(768 * 128);
OPTIMIZE TABLE t_read_end_tasks FINAL;

SET max_threads = 2, max_threads_min_free_memory_per_thread = 0, max_streams_to_max_threads_ratio = 1;
SET max_block_size = 8192, preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0;
-- These give a default task of 24 marks and a smallest task of 8 marks, as the default settings do for
-- granules of 8,192 rows.
SET merge_tree_min_rows_for_concurrent_read = 3072, merge_tree_min_bytes_for_concurrent_read = 0;
SET merge_tree_min_read_task_size = 8;
SET merge_tree_min_bytes_per_read_stream = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET allow_prefetched_read_pool_for_local_filesystem = 0, use_columns_cache = 0;
-- The end of the read is counted in tasks per thread of the pool, so the pool keeps all its threads.
SET read_backoff_min_latency_ms = 0;

-- All tasks are default ones: 32 blocks of 3,072 rows.
SELECT 'off';
SELECT rows_in_block, count()
FROM (SELECT blockSize() AS rows_in_block, rowNumberInBlock() AS row_in_block FROM t_read_end_tasks)
WHERE row_in_block = 0 GROUP BY rows_in_block ORDER BY rows_in_block
SETTINGS merge_tree_read_end_tasks_per_stream = 0;

-- A task at the end of the read is 384 / 48 = 8 marks: 30 blocks of 3,072 rows and 6 blocks of 1,024 rows.
SELECT '48 tasks per stream';
SELECT rows_in_block, count()
FROM (SELECT blockSize() AS rows_in_block, rowNumberInBlock() AS row_in_block FROM t_read_end_tasks)
WHERE row_in_block = 0 GROUP BY rows_in_block ORDER BY rows_in_block
SETTINGS merge_tree_read_end_tasks_per_stream = 48;

DROP TABLE t_read_end_tasks;
