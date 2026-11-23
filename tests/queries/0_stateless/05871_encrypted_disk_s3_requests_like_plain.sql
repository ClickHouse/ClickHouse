-- Tags: no-fasttest, no-distributed-cache
-- no-fasttest: needs MinIO.
-- no-distributed-cache: reads through the distributed cache do not make these S3 requests.

-- Reading from an encrypted disk must make as many S3 requests as reading the same data from the disk under it.
-- The encryption header used to be read with an unbounded request, which was reopened when the reader set its range.
-- Parts in packed storage are excluded with `min_bytes_for_full_part_storage = 0`: a file inside the packed archive
-- does not start at the archive's header, so its header is read with a request of its own.
-- Nothing may load the marks or the primary key before the queries, so that both queries load them themselves:
-- otherwise the shared caches decide whether a query reads them.

DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_encrypted;

CREATE TABLE t_plain (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS storage_policy = 's3_no_cache', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    prewarm_mark_cache = 0, prewarm_primary_key_cache = 0, use_primary_key_cache = 0, index_granularity = 1024;

CREATE TABLE t_encrypted (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS storage_policy = 's3_no_cache_encrypted', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    prewarm_mark_cache = 0, prewarm_primary_key_cache = 0, use_primary_key_cache = 0, index_granularity = 1024;

INSERT INTO t_plain SELECT number, toString(number) FROM numbers(100000);
INSERT INTO t_plain SELECT number, toString(number) FROM numbers(100000, 100000);
INSERT INTO t_encrypted SELECT number, toString(number) FROM numbers(100000);
INSERT INTO t_encrypted SELECT number, toString(number) FROM numbers(100000, 100000);
OPTIMIZE TABLE t_plain FINAL;
OPTIMIZE TABLE t_encrypted FINAL;

-- A mark range, with the randomized settings that change how the data is read pinned.
SELECT sum(length(s)) FROM t_plain WHERE k BETWEEN 10000 AND 60000
SETTINGS max_threads = 1, remote_filesystem_read_method = 'threadpool', remote_filesystem_read_prefetch = 1, use_uncompressed_cache = 0,
    use_page_cache_for_object_storage = 0, use_page_cache_for_disks_without_file_cache = 0, use_columns_cache = 0,
    enable_parallel_replicas = 0, log_comment = '05871_plain';
SELECT sum(length(s)) FROM t_encrypted WHERE k BETWEEN 10000 AND 60000
SETTINGS max_threads = 1, remote_filesystem_read_method = 'threadpool', remote_filesystem_read_prefetch = 1, use_uncompressed_cache = 0,
    use_page_cache_for_object_storage = 0, use_page_cache_for_disks_without_file_cache = 0, use_columns_cache = 0,
    enable_parallel_replicas = 0, log_comment = '05871_encrypted';

SYSTEM FLUSH LOGS part_log, query_log;

SELECT
    sumIf(ProfileEvents['S3GetObject'], table = 't_plain') > 0,
    sumIf(ProfileEvents['S3GetObject'], table = 't_encrypted') = sumIf(ProfileEvents['S3GetObject'], table = 't_plain')
FROM system.part_log
WHERE database = currentDatabase() AND event_type = 'MergeParts';

SELECT
    sumIf(ProfileEvents['S3GetObject'], log_comment = '05871_plain') > 0,
    sumIf(ProfileEvents['S3GetObject'], log_comment = '05871_encrypted') = sumIf(ProfileEvents['S3GetObject'], log_comment = '05871_plain')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05871_plain', '05871_encrypted');

DROP TABLE t_plain;
DROP TABLE t_encrypted;
