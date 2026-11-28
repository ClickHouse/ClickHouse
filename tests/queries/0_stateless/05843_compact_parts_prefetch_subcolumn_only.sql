-- Tags: no-random-settings, no-random-merge-tree-settings

-- A query that reads only a subcolumn seeks to the marks of the substreams of this subcolumn.
-- The prefetch of a Compact part must start at the first of these substreams, not at the first substream
-- of the column: otherwise it reads a large leading subcolumn that the query does not need.
-- Checked for both the buffer shared by all columns (a part without stripes) and the buffer per column (stripes).

DROP TABLE IF EXISTS t_compact_prefetch_subcolumn_shared;
DROP TABLE IF EXISTS t_compact_prefetch_subcolumn_stripes;

CREATE TABLE t_compact_prefetch_subcolumn_shared (k UInt64, t Tuple(s String, b UInt64))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 128, min_bytes_for_wide_part = '10G', compact_parts_max_granules_to_buffer = 1, write_marks_for_substreams_in_compact_parts = 1, storage_policy = 'default';

CREATE TABLE t_compact_prefetch_subcolumn_stripes (k UInt64, t Tuple(s String, b UInt64))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 128, min_bytes_for_wide_part = '10G', compact_parts_max_granules_to_buffer = 8, compact_parts_max_bytes_to_buffer = '128Mi', write_marks_for_substreams_in_compact_parts = 1, storage_policy = 'default';

INSERT INTO t_compact_prefetch_subcolumn_shared SELECT number, (randomString(8192), number) FROM numbers(4096);
INSERT INTO t_compact_prefetch_subcolumn_stripes SELECT number, (randomString(8192), number) FROM numbers(4096);

SET allow_prefetched_read_pool_for_local_filesystem = 1, local_filesystem_read_method = 'pread_threadpool',
    use_reader_executor = 0, filesystem_prefetches_limit = 0, filesystem_prefetch_max_memory_usage = '1Gi', filesystem_prefetch_step_marks = 1,
    merge_tree_min_read_task_size = 1, merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1,
    merge_tree_compact_parts_min_granules_to_multibuffer_read = 1,
    max_threads = 2, use_uncompressed_cache = 0, use_query_condition_cache = 0, optimize_use_implicit_projections = 0,
    enable_parallel_replicas = 0, use_statistics_for_sum_avg_aggregation = 0;

SELECT sum(t.b) FROM t_compact_prefetch_subcolumn_shared SETTINGS local_filesystem_read_prefetch = 0, log_comment = '05843_shared_no_prefetch';
SELECT sum(t.b) FROM t_compact_prefetch_subcolumn_shared SETTINGS local_filesystem_read_prefetch = 1, log_comment = '05843_shared_prefetch';
SELECT sum(t.b) FROM t_compact_prefetch_subcolumn_stripes SETTINGS local_filesystem_read_prefetch = 0, log_comment = '05843_stripes_no_prefetch';
SELECT sum(t.b) FROM t_compact_prefetch_subcolumn_stripes SETTINGS local_filesystem_read_prefetch = 1, log_comment = '05843_stripes_prefetch';

SYSTEM FLUSH LOGS query_log;

-- The prefetch does not read the subcolumn `t.s`: the queries read about the same number of bytes.
SELECT
    sumIf(ProfileEvents['ReadBufferFromFileDescriptorReadBytes'], log_comment = '05843_shared_prefetch')
        < 1.5 * sumIf(ProfileEvents['ReadBufferFromFileDescriptorReadBytes'], log_comment = '05843_shared_no_prefetch'),
    sumIf(ProfileEvents['ReadBufferFromFileDescriptorReadBytes'], log_comment = '05843_stripes_prefetch')
        < 1.5 * sumIf(ProfileEvents['ReadBufferFromFileDescriptorReadBytes'], log_comment = '05843_stripes_no_prefetch')
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment LIKE '05843_%' AND query_kind = 'Select';

DROP TABLE t_compact_prefetch_subcolumn_shared;
DROP TABLE t_compact_prefetch_subcolumn_stripes;
