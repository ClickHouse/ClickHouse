-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
-- no-fasttest: needs S3

-- When `PREWHERE` reads all the columns of a task, the main reader of the task has no columns to read.
-- Such a reader of a Compact part must not prefetch the data file, otherwise the part is read twice.

DROP TABLE IF EXISTS t_compact_prefetch_empty_reader;

CREATE TABLE t_compact_prefetch_empty_reader (a UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = '10G', storage_policy = 's3_no_cache';

INSERT INTO t_compact_prefetch_empty_reader SELECT number FROM numbers(65536);

SET allow_prefetched_read_pool_for_remote_filesystem = 1, remote_filesystem_read_method = 'threadpool',
    use_reader_executor = 0, filesystem_prefetches_limit = 0, filesystem_prefetch_max_memory_usage = '1Gi', remote_filesystem_read_prefetch = 1,
    merge_tree_compact_parts_min_granules_to_multibuffer_read = 1, max_threads = 1, enable_parallel_replicas = 0;

SELECT count() FROM t_compact_prefetch_empty_reader PREWHERE a % 2 = 0 SETTINGS log_comment = '05408_prefetch';

SYSTEM FLUSH LOGS query_log;

-- The data file is read once.
WITH (SELECT sum(bytes_on_disk) FROM system.parts WHERE database = currentDatabase() AND table = 't_compact_prefetch_empty_reader' AND active) AS bytes
SELECT ProfileEvents['ReadBufferFromS3Bytes'] BETWEEN 0.5 * bytes AND 1.5 * bytes
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment = '05408_prefetch' AND query_kind = 'Select';

DROP TABLE t_compact_prefetch_empty_reader;
