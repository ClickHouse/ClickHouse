-- Tags: no-random-settings, no-random-merge-tree-settings

-- A Compact part without stripes is read with one buffer for all columns.
-- Its prefetch must start at the first column to read, not at the first column of the part:
-- otherwise it reads a large leading column that the query does not need.

DROP TABLE IF EXISTS t_compact_shared_prefetch;

CREATE TABLE t_compact_shared_prefetch (s String, b UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 128, min_bytes_for_wide_part = '10G', compact_parts_max_granules_to_buffer = 1, storage_policy = 'default';

INSERT INTO t_compact_shared_prefetch SELECT randomString(8192), number FROM numbers(4096);

SET allow_prefetched_read_pool_for_local_filesystem = 1, local_filesystem_read_method = 'pread_threadpool',
    use_reader_executor = 0, filesystem_prefetches_limit = 0, filesystem_prefetch_max_memory_usage = '1Gi', filesystem_prefetch_step_marks = 1,
    merge_tree_min_read_task_size = 1, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1,
    max_threads = 2, use_uncompressed_cache = 0, use_query_condition_cache = 0, optimize_use_implicit_projections = 0,
    enable_parallel_replicas = 0, use_statistics_for_sum_avg_aggregation = 0;

SELECT sum(b) FROM t_compact_shared_prefetch SETTINGS local_filesystem_read_prefetch = 0, log_comment = '05828_no_prefetch';
SELECT sum(b) FROM t_compact_shared_prefetch SETTINGS local_filesystem_read_prefetch = 1, log_comment = '05828_prefetch';

SYSTEM FLUSH LOGS query_log;

-- The prefetch does not read the column `s`: both queries read about the same number of bytes.
SELECT
    sumIf(ProfileEvents['ReadBufferFromFileDescriptorReadBytes'], log_comment = '05828_prefetch')
        < 1.5 * sumIf(ProfileEvents['ReadBufferFromFileDescriptorReadBytes'], log_comment = '05828_no_prefetch')
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment IN ('05828_prefetch', '05828_no_prefetch') AND query_kind = 'Select';

DROP TABLE t_compact_shared_prefetch;
