-- Tags: no-fasttest, no-distributed-cache
-- no-fasttest: needs MinIO.
-- no-distributed-cache: reads through the distributed cache do not make these S3 requests.

-- Reading a part in packed storage from an encrypted disk must make about as many S3 requests as from the disk under it.
-- Every file inside the packed archive used to read the archive's encryption header with a request of its own. Now the
-- header is cached by the blob path, so only the first read of an archive reads it. The allowed difference covers those
-- first reads; without the cache the difference is about one request per file.

DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_encrypted;

-- Merges are pinned horizontal. In CI, a vertical merge of a packed part sometimes opens more files of the archive on the
-- encrypted disk for the first time, and each of them reads the header, which is more than the allowed difference covers.
CREATE TABLE t_plain (k UInt64, s1 String, s2 String, s3 String, n1 UInt64, n2 UInt64, n3 UInt64, n4 UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS storage_policy = 's3_no_cache', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 1000000000000,
    prewarm_mark_cache = 0, prewarm_primary_key_cache = 0, use_primary_key_cache = 0, index_granularity = 1024,
    enable_vertical_merge_algorithm = 0;

CREATE TABLE t_encrypted (k UInt64, s1 String, s2 String, s3 String, n1 UInt64, n2 UInt64, n3 UInt64, n4 UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS storage_policy = 's3_no_cache_encrypted', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 1000000000000,
    prewarm_mark_cache = 0, prewarm_primary_key_cache = 0, use_primary_key_cache = 0, index_granularity = 1024,
    enable_vertical_merge_algorithm = 0;

-- One part per table, so that no background merge runs before `OPTIMIZE` and each table has exactly one merge.
INSERT INTO t_plain SELECT number, toString(number), toString(number * 3), toString(number * 7), number, number * 3, number * 7, number * 11 FROM numbers(200000)
SETTINGS max_insert_threads = 1;
INSERT INTO t_encrypted SELECT number, toString(number), toString(number * 3), toString(number * 7), number, number * 3, number * 7, number * 11 FROM numbers(200000)
SETTINGS max_insert_threads = 1;
OPTIMIZE TABLE t_plain FINAL;
OPTIMIZE TABLE t_encrypted FINAL;

-- All columns, with the randomized settings that change how the data is read pinned.
SELECT count(), sum(cityHash64(*)) FROM t_plain
SETTINGS max_threads = 1, remote_filesystem_read_method = 'threadpool', remote_filesystem_read_prefetch = 1, use_uncompressed_cache = 0,
    use_page_cache_for_object_storage = 0, use_page_cache_for_disks_without_file_cache = 0, use_columns_cache = 0,
    enable_parallel_replicas = 0, log_comment = '05872_plain';
SELECT count(), sum(cityHash64(*)) FROM t_encrypted
SETTINGS max_threads = 1, remote_filesystem_read_method = 'threadpool', remote_filesystem_read_prefetch = 1, use_uncompressed_cache = 0,
    use_page_cache_for_object_storage = 0, use_page_cache_for_disks_without_file_cache = 0, use_columns_cache = 0,
    enable_parallel_replicas = 0, log_comment = '05872_encrypted';

SYSTEM FLUSH LOGS part_log, query_log;

SELECT
    sumIf(ProfileEvents['S3GetObject'], table = 't_plain') > 0,
    sumIf(ProfileEvents['S3GetObject'], table = 't_encrypted') <= sumIf(ProfileEvents['S3GetObject'], table = 't_plain') + 4
FROM system.part_log
WHERE database = currentDatabase() AND event_type = 'MergeParts';

SELECT
    sumIf(ProfileEvents['S3GetObject'], log_comment = '05872_plain') > 0,
    sumIf(ProfileEvents['S3GetObject'], log_comment = '05872_encrypted') <= sumIf(ProfileEvents['S3GetObject'], log_comment = '05872_plain') + 4
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05872_plain', '05872_encrypted');

DROP TABLE t_plain;
DROP TABLE t_encrypted;
