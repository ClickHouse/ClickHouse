-- Tags: no-fasttest, no-distributed-cache
-- no-fasttest: needs MinIO.
-- no-distributed-cache: reads through the distributed cache do not use remote prefetches.

-- Merges and queries on an encrypted remote disk must not cancel their prefetches. `ReadBufferFromEncryptedFile::prefetch`
-- used to start the inner prefetch before applying the pending seek and read range, so the first read discarded it.

DROP TABLE IF EXISTS t_encrypted_prefetch;

CREATE TABLE t_encrypted_prefetch (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS storage_policy = 's3_no_cache_encrypted', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_encrypted_prefetch SELECT number, toString(number) FROM numbers(100000);
INSERT INTO t_encrypted_prefetch SELECT number, toString(number) FROM numbers(100000, 100000);
OPTIMIZE TABLE t_encrypted_prefetch FINAL;

-- Pin the randomized settings that could disable remote prefetches.
SELECT count(), sum(k), sum(length(s)) FROM t_encrypted_prefetch
SETTINGS remote_filesystem_read_method = 'threadpool', remote_filesystem_read_prefetch = 1, use_uncompressed_cache = 0,
    use_page_cache_for_object_storage = 0, use_page_cache_for_disks_without_file_cache = 0, log_comment = '05813_select';

SYSTEM FLUSH LOGS part_log, query_log;

SELECT sum(ProfileEvents['RemoteFSPrefetches']) > 0, sum(ProfileEvents['RemoteFSCancelledPrefetches'])
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_encrypted_prefetch' AND event_type = 'MergeParts';

SELECT ProfileEvents['RemoteFSPrefetches'] > 0, ProfileEvents['RemoteFSCancelledPrefetches']
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05813_select' AND type = 'QueryFinish';

DROP TABLE t_encrypted_prefetch;
