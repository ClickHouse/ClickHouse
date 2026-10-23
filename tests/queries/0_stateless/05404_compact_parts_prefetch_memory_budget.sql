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
    merge_tree_compact_parts_min_granules_to_multibuffer_read = 1, max_threads = 2, enable_parallel_replicas = 0,
    prefetch_buffer_size = 1048576;

SELECT sum(c2), sum(c3) FROM t_compact_prefetch_budget SETTINGS filesystem_prefetch_max_memory_usage = 1, log_comment = '05404_small_budget';
SELECT sum(c2), sum(c3) FROM t_compact_prefetch_budget SETTINGS filesystem_prefetch_max_memory_usage = '1Gi', log_comment = '05404_large_budget';

-- With a pending `RENAME COLUMN` the part has no column with the requested name, but the reader reads it by the old name.
SYSTEM STOP MERGES t_compact_prefetch_budget;
ALTER TABLE t_compact_prefetch_budget RENAME COLUMN c2 TO renamed_c2 SETTINGS alter_sync = 0;
SELECT sum(renamed_c2) FROM t_compact_prefetch_budget SETTINGS filesystem_prefetch_max_memory_usage = 1, log_comment = '05404_renamed_small_budget';
SELECT sum(renamed_c2) FROM t_compact_prefetch_budget SETTINGS filesystem_prefetch_max_memory_usage = '1Gi', log_comment = '05404_renamed_large_budget';

DROP TABLE t_compact_prefetch_budget;

-- The columns of a Compact part can have very different sizes: here one column takes almost the whole part. A budget above
-- the average size of a column, but below the buffer of the big column, must not allow to prefetch it.
CREATE TABLE t_compact_prefetch_budget (c1 UInt32, s String,
    d1 UInt8, d2 UInt8, d3 UInt8, d4 UInt8, d5 UInt8, d6 UInt8, d7 UInt8, d8 UInt8, d9 UInt8, d10 UInt8,
    d11 UInt8, d12 UInt8, d13 UInt8, d14 UInt8, d15 UInt8, d16 UInt8, d17 UInt8, d18 UInt8, d19 UInt8, d20 UInt8,
    d21 UInt8, d22 UInt8, d23 UInt8, d24 UInt8, d25 UInt8, d26 UInt8, d27 UInt8, d28 UInt8, d29 UInt8, d30 UInt8,
    d31 UInt8, d32 UInt8, d33 UInt8, d34 UInt8, d35 UInt8, d36 UInt8, d37 UInt8, d38 UInt8)
ENGINE = MergeTree ORDER BY c1
SETTINGS index_granularity = 512, min_bytes_for_wide_part = '10G', storage_policy = 's3_no_cache',
    compact_parts_max_granules_to_buffer = 128, compact_parts_max_bytes_to_buffer = '128Mi';

INSERT INTO t_compact_prefetch_budget (c1, s) SELECT number, randomString(64) FROM numbers(512 * 64);

-- The data file is about 2 MiB, so the average size of a column is about 55 KB.
SELECT sum(length(s)) FROM t_compact_prefetch_budget SETTINGS filesystem_prefetch_max_memory_usage = 200000, log_comment = '05404_uneven_small_budget';
SELECT sum(length(s)) FROM t_compact_prefetch_budget SETTINGS filesystem_prefetch_max_memory_usage = '1Gi', log_comment = '05404_uneven_large_budget';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, asynchronous_read_counters['total_prefetch_tasks'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment LIKE '05404\\_%' AND query_kind = 'Select'
ORDER BY log_comment DESC;

DROP TABLE t_compact_prefetch_budget;
