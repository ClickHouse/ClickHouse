-- Tags: no-fasttest, no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas, no-replicated-database
-- - no-fasttest -- requires S3
-- - no-parallel -- `SYSTEM DROP COLUMNS CACHE` is server-wide
-- - no-random-settings, no-random-merge-tree-settings -- the read method, the prefetch flag, the part type and
--   the stripe layout have to be deterministic
-- - no-parallel-replicas -- another replica would do the reading, and the prefetching
-- - no-replicated-database -- the cache is per server

-- The same as `05154_columns_cache_remote_prefetch_skip`, but for a Compact part read with a buffer per column
-- (`MergeTreeReaderCompactMultipleBuffers`): a read that the columns cache serves as a whole must not prefetch
-- the streams of the range first.

DROP TABLE IF EXISTS t_compact_cc_prefetch;

CREATE TABLE t_compact_cc_prefetch (a UInt64, b UInt64)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS disk = 's3_no_cache', min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000,
    index_granularity = 512, compact_parts_max_granules_to_buffer = 128, compact_parts_max_bytes_to_buffer = '128Mi';

INSERT INTO t_compact_cc_prefetch SELECT number, number FROM numbers(32768);

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact_cc_prefetch' AND active;

SYSTEM DROP COLUMNS CACHE;

SELECT sum(b) FROM t_compact_cc_prefetch SETTINGS
    use_columns_cache = 1,
    enable_writes_to_columns_cache = 1,
    enable_reads_from_columns_cache = 1,
    allow_prefetched_read_pool_for_remote_filesystem = 1,
    remote_filesystem_read_method = 'threadpool',
    remote_filesystem_read_prefetch = 1,
    merge_tree_compact_parts_min_granules_to_multibuffer_read = 1,
    max_threads = 1,
    log_comment = '05454_cold';

SELECT sum(b) FROM t_compact_cc_prefetch SETTINGS
    use_columns_cache = 1,
    enable_writes_to_columns_cache = 1,
    enable_reads_from_columns_cache = 1,
    allow_prefetched_read_pool_for_remote_filesystem = 1,
    remote_filesystem_read_method = 'threadpool',
    remote_filesystem_read_prefetch = 1,
    merge_tree_compact_parts_min_granules_to_multibuffer_read = 1,
    max_threads = 1,
    log_comment = '05454_hot';

SYSTEM FLUSH LOGS query_log;

-- The cold read must have prefetched and missed, the hot one must have hit, not missed, and not prefetched.
SELECT
    log_comment,
    ProfileEvents['ColumnsCacheHits'] > 0 AS served_from_cache,
    ProfileEvents['ColumnsCacheMisses'] > 0 AS missed,
    ProfileEvents['RemoteFSPrefetches'] > 0 AS prefetched
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment IN ('05454_cold', '05454_hot')
  AND type = 'QueryFinish'
  AND event_time >= now() - INTERVAL 5 MINUTE
ORDER BY log_comment;

DROP TABLE t_compact_cc_prefetch;
