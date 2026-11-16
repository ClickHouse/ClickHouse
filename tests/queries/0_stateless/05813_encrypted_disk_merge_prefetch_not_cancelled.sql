-- Tags: no-fasttest, no-distributed-cache
-- no-fasttest: needs MinIO.
-- no-distributed-cache: reads through the distributed cache do not use remote prefetches.

-- A merge on an encrypted remote disk must not cancel its prefetches. `ReadBufferFromEncryptedFile::prefetch`
-- used to start the inner prefetch before applying the pending read range, so the first read discarded it.

DROP TABLE IF EXISTS t_encrypted_prefetch;

CREATE TABLE t_encrypted_prefetch (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS storage_policy = 's3_no_cache_encrypted', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_encrypted_prefetch SELECT number, toString(number) FROM numbers(100000);
INSERT INTO t_encrypted_prefetch SELECT number, toString(number) FROM numbers(100000, 100000);
OPTIMIZE TABLE t_encrypted_prefetch FINAL;

SELECT count(), sum(k), sum(length(s)) FROM t_encrypted_prefetch;

SYSTEM FLUSH LOGS part_log;

SELECT sum(ProfileEvents['RemoteFSPrefetches']) > 0, sum(ProfileEvents['RemoteFSCancelledPrefetches'])
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_encrypted_prefetch' AND event_type = 'MergeParts';

DROP TABLE t_encrypted_prefetch;
