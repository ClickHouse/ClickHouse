-- Tags: no-parallel-replicas, no-object-storage
-- The task size is lowered only for a local read from the plain read pool.

-- `merge_tree_min_read_task_bytes` lets the tasks at the end of a `MergeTree` read of heavy marks be smaller than
-- `merge_tree_min_read_task_size`: the smallest task is the number of marks that hold that many bytes of the
-- columns being read, but at least one mark. 0 keeps `merge_tree_min_read_task_size` as a hard limit.
-- A block never spans two tasks, so with `max_block_size` above the rows of a task every block is one task, and
-- `blockSize` shows the task size. The table has 96 granules of 128 rows in one part, and a mark of `s` holds
-- about 13 KB. 2 streams have 48 marks each, which is 2 default tasks of 24 marks, and the end of the read is
-- the last 2 * 24 marks, whose tasks are 48 / 32 marks, raised to the smallest task.

DROP TABLE IF EXISTS t_read_task_bytes;
CREATE TABLE t_read_task_bytes (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_read_task_bytes SELECT number, repeat('x', 100) FROM numbers(96 * 128);
OPTIMIZE TABLE t_read_task_bytes FINAL;

SET max_threads = 2, max_threads_min_free_memory_per_thread = 0, max_streams_to_max_threads_ratio = 1;
SET max_block_size = 8192, preferred_block_size_bytes = 0, preferred_max_column_in_block_size_bytes = 0;
SET merge_tree_min_rows_for_concurrent_read = 3072, merge_tree_min_bytes_for_concurrent_read = 0;
SET merge_tree_min_read_task_size = 8, merge_tree_read_end_tasks_per_stream = 32;
SET merge_tree_min_bytes_per_read_stream = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET allow_prefetched_read_pool_for_local_filesystem = 0, use_columns_cache = 0;
SET read_backoff_min_latency_ms = 0;
SET optimize_functions_to_subcolumns = 0;

-- The hard limit: the end of the read is cut into tasks of 8 marks.
SELECT '0';
SELECT rows_in_block, count(), max(length(s))
FROM (SELECT blockSize() AS rows_in_block, rowNumberInBlock() AS row_in_block, s FROM t_read_task_bytes)
WHERE row_in_block = 0 GROUP BY rows_in_block ORDER BY rows_in_block
SETTINGS merge_tree_min_read_task_bytes = 0;

-- The marks are light for the default, so the tasks are the same.
SELECT 'default';
SELECT rows_in_block, count(), max(length(s))
FROM (SELECT blockSize() AS rows_in_block, rowNumberInBlock() AS row_in_block, s FROM t_read_task_bytes)
WHERE row_in_block = 0 GROUP BY rows_in_block ORDER BY rows_in_block;

-- About 13 KB per mark: 20,000 bytes are 2 marks.
SELECT '20000';
SELECT rows_in_block, count(), max(length(s))
FROM (SELECT blockSize() AS rows_in_block, rowNumberInBlock() AS row_in_block, s FROM t_read_task_bytes)
WHERE row_in_block = 0 GROUP BY rows_in_block ORDER BY rows_in_block
SETTINGS merge_tree_min_read_task_bytes = 20000;

-- A task is at least one mark.
SELECT '1';
SELECT rows_in_block, count(), max(length(s))
FROM (SELECT blockSize() AS rows_in_block, rowNumberInBlock() AS row_in_block, s FROM t_read_task_bytes)
WHERE row_in_block = 0 GROUP BY rows_in_block ORDER BY rows_in_block
SETTINGS merge_tree_min_read_task_bytes = 1;

DROP TABLE t_read_task_bytes;
