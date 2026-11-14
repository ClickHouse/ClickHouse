-- Tags: no-random-settings, no-random-merge-tree-settings, no-fasttest
-- Tag no-fasttest: Depends on Parquet

-- A data part in Parquet format is read in multiple tasks by the same reader without the columns cache:
-- the reader initializes the row groups of the mark ranges of each next task.

SET allow_experimental_parquet_parts = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_parquet_tasks_no_cache;

CREATE TABLE t_parquet_tasks_no_cache (id UInt64, value String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_parquet_part = 1, parquet_row_group_granularity_rows = 1000;

INSERT INTO t_parquet_tasks_no_cache SELECT number, toString(number) FROM numbers(100000);

SELECT part_type, marks FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_tasks_no_cache' AND active;

SELECT sum(id), sum(length(value)) FROM t_parquet_tasks_no_cache
SETTINGS use_columns_cache = 0, max_threads = 2, max_block_size = 1000, merge_tree_min_read_task_size = 1,
    merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 0,
    use_indexes_refiner_in_read_pools = 0, use_statistics_for_sum_avg_aggregation = 0;

SELECT sum(id), sum(length(value)) FROM t_parquet_tasks_no_cache WHERE id % 3 = 1
SETTINGS use_columns_cache = 0, max_threads = 4, max_block_size = 100, merge_tree_min_read_task_size = 1,
    merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 0,
    use_indexes_refiner_in_read_pools = 0, use_statistics_for_sum_avg_aggregation = 0;

DROP TABLE t_parquet_tasks_no_cache;
