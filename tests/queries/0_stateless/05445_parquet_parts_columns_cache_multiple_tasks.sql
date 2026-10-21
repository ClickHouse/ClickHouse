-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database, no-fasttest
-- Tag no-parallel: drops the columns cache.
-- Tag no-fasttest: Depends on Parquet

-- The columns cache serves all row groups of a data part in Parquet format, when the part is read
-- in multiple tasks by the same reader.

SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_parquet_cache_tasks;

CREATE TABLE t_parquet_cache_tasks (id UInt64, value String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_parquet_part = 1, parquet_row_group_granularity_rows = 1000;

INSERT INTO t_parquet_cache_tasks SELECT number, toString(number) FROM numbers(100000);

SYSTEM DROP COLUMNS CACHE;

SELECT part_type, marks FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_cache_tasks' AND active;

SELECT sum(id), sum(length(value)) FROM t_parquet_cache_tasks
SETTINGS use_columns_cache = 1, max_threads = 2, max_block_size = 1000, merge_tree_min_read_task_size = 1,
    merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 0;

SELECT column, count(), sum(rows) FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_parquet_cache_tasks' GROUP BY column ORDER BY column;

DROP TABLE t_parquet_cache_tasks;
