-- Tags: no-parallel-replicas, no-object-storage
-- The task size is lowered only for a local read from the plain read pool.

-- A stream of a `MergeTree` read gets a share of the marks and reads it in tasks. A stream that has read its
-- share takes tasks from the shares of other streams, so the end of the read, one default task per stream, is
-- cut into smaller tasks: 1/32 of a share, not below `merge_tree_min_read_task_size`.
--
-- A block never spans two tasks, so with `max_block_size` above the rows of a task every block is one task,
-- and `blockSize` shows the task size. The table has 768 granules of 128 rows in one part. In every read
-- below the marks cut into default tasks and the marks cut into smaller tasks are both a whole number of
-- tasks, so the blocks do not depend on which stream reads which task.

DROP TABLE IF EXISTS t_read_task_size;
CREATE TABLE t_read_task_size (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_read_task_size SELECT number FROM numbers(768 * 128);
OPTIMIZE TABLE t_read_task_size FINAL;

SET max_threads_min_free_memory_per_thread = 0, max_streams_to_max_threads_ratio = 1;
SET max_block_size = 8192, preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0;
-- These give a default task of 24 marks and a smallest task of 8 marks, as the default settings do for
-- granules of 8,192 rows.
SET merge_tree_min_rows_for_concurrent_read = 3072, merge_tree_min_bytes_for_concurrent_read = 0;
SET merge_tree_min_read_task_size = 8;
-- Pin (randomized in CI): the end of the read is cut into 1/32 of a share, the default.
SET merge_tree_read_end_tasks_per_stream = 32;
SET merge_tree_min_bytes_per_read_stream = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET allow_prefetched_read_pool_for_local_filesystem = 0, use_columns_cache = 0;
-- The end of the read is counted in tasks per thread of the pool, so the pool keeps all its threads.
SET read_backoff_min_latency_ms = 0;

-- The marks of the part: 768 granules and the final mark.
SELECT sum(marks) FROM system.parts WHERE database = currentDatabase() AND table = 't_read_task_size' AND active;

-- 2 streams have 384 marks each, which is 16 default tasks, so a task at the end of the read is 384 / 32 = 12
-- marks. The last 2 * 24 marks are read in such tasks: 30 blocks of 3,072 rows and 4 blocks of 1,536 rows.
SELECT 'a share of 16 default tasks';
SELECT rows_in_block, count()
FROM (SELECT blockSize() AS rows_in_block, rowNumberInBlock() AS row_in_block FROM t_read_task_size)
WHERE row_in_block = 0 GROUP BY rows_in_block ORDER BY rows_in_block
SETTINGS max_threads = 2;

-- 8 streams have 96 marks each, which is 4 default tasks. 96 / 32 = 3 marks is below the smallest task, so a
-- task at the end of the read is 8 marks. The last 8 * 24 marks are read in such tasks: 24 blocks of 3,072
-- rows and 24 blocks of 1,024 rows.
SELECT 'a share of 4 default tasks';
SELECT rows_in_block, count()
FROM (SELECT blockSize() AS rows_in_block, rowNumberInBlock() AS row_in_block FROM t_read_task_size)
WHERE row_in_block = 0 GROUP BY rows_in_block ORDER BY rows_in_block
SETTINGS max_threads = 8;

-- Here the default task is 4 marks and the smallest task is 1 mark. 2 streams have 384 marks each, which is
-- 96 default tasks, so all tasks are default ones: 192 blocks of 512 rows.
SELECT 'a share of 96 default tasks';
SELECT rows_in_block, count()
FROM (SELECT blockSize() AS rows_in_block, rowNumberInBlock() AS row_in_block FROM t_read_task_size)
WHERE row_in_block = 0 GROUP BY rows_in_block ORDER BY rows_in_block
SETTINGS max_threads = 2, merge_tree_min_rows_for_concurrent_read = 512, merge_tree_min_read_task_size = 1;

DROP TABLE t_read_task_size;
