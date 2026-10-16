-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
-- no-fasttest: needs S3

-- The prefetched read pool must account the buffers of a Compact part in `filesystem_prefetch_max_memory_usage`.
-- A Compact part has no sizes of individual columns, so its tasks used to be admitted for prefetching for free.

DROP TABLE IF EXISTS t_compact_prefetch_budget;

CREATE TABLE t_compact_prefetch_budget (c1 UInt32, c2 UInt32, c3 UInt32, c4 UInt32)
ENGINE = MergeTree ORDER BY c1
SETTINGS index_granularity = 512, min_bytes_for_wide_part = '10G', storage_policy = 's3_no_cache',
    compact_parts_max_granules_to_buffer = 128, compact_parts_max_bytes_to_buffer = '128Mi';

INSERT INTO t_compact_prefetch_budget SELECT number, number, number, number FROM numbers(512 * 64);

SET allow_prefetched_read_pool_for_remote_filesystem = 1, remote_filesystem_read_method = 'threadpool',
    remote_filesystem_read_prefetch = 1, use_reader_executor = 0, filesystem_prefetches_limit = 0,
    merge_tree_compact_parts_min_granules_to_multibuffer_read = 1, max_threads = 2;

SELECT sum(c2), sum(c3) FROM t_compact_prefetch_budget SETTINGS filesystem_prefetch_max_memory_usage = 1, log_comment = '05404_small_budget';
SELECT sum(c2), sum(c3) FROM t_compact_prefetch_budget SETTINGS filesystem_prefetch_max_memory_usage = '1Gi', log_comment = '05404_large_budget';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, asynchronous_read_counters['total_prefetch_tasks'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment LIKE '05404\\_%' AND query_kind = 'Select'
ORDER BY log_comment DESC;

DROP TABLE t_compact_prefetch_budget;
