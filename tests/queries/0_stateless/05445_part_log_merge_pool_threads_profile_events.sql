-- Tags: no-fasttest
-- no-fasttest: needs an S3 disk.

-- `ProfileEvents` of a merge in `system.part_log` include the work of all threads of the merge.
-- A vertical merge on object storage prefetches the columns, so the reads run on the threads of
-- the remote reader pool. `OPTIMIZE` executes the merge and gets the counters of all its threads,
-- so the merge row in `system.part_log` must show the same reads as the `OPTIMIZE` query.

DROP TABLE IF EXISTS t_part_log_merge_pool_threads;

-- `max_bytes_to_merge_at_max_space_in_pool = 1` disables background merges; `OPTIMIZE FINAL` ignores it.
CREATE TABLE t_part_log_merge_pool_threads (id UInt64, c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 String)
ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 's3_no_cache', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_remote_filesystem_prefetch = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_part_log_merge_pool_threads SELECT number, number, number * 2, number * 3, number * 4, toString(number) FROM numbers(10000);
INSERT INTO t_part_log_merge_pool_threads SELECT number, number, number * 2, number * 3, number * 4, toString(number) FROM numbers(10000, 10000);
INSERT INTO t_part_log_merge_pool_threads SELECT number, number, number * 2, number * 3, number * 4, toString(number) FROM numbers(20000, 10000);

OPTIMIZE TABLE t_part_log_merge_pool_threads FINAL;

SYSTEM FLUSH LOGS part_log, query_log;

SELECT
    merge.algorithm,
    merge.get_requests > 0,
    merge.get_requests = optimize.get_requests,
    merge.read_bytes = optimize.read_bytes
FROM
(
    SELECT
        merge_algorithm AS algorithm,
        ProfileEvents['S3GetObject'] AS get_requests,
        ProfileEvents['ReadBufferFromS3Bytes'] AS read_bytes
    FROM system.part_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND database = currentDatabase() AND table = 't_part_log_merge_pool_threads' AND event_type = 'MergeParts'
) AS merge,
(
    SELECT
        ProfileEvents['S3GetObject'] AS get_requests,
        ProfileEvents['ReadBufferFromS3Bytes'] AS read_bytes
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND current_database = currentDatabase() AND type = 'QueryFinish'
        AND query LIKE 'OPTIMIZE TABLE t_part_log_merge_pool_threads FINAL%'
) AS optimize;

DROP TABLE t_part_log_merge_pool_threads;
