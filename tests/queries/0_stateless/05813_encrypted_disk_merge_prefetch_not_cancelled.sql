-- Tags: no-fasttest, no-distributed-cache
-- no-fasttest: needs MinIO.
-- no-distributed-cache: reads through the distributed cache do not use remote prefetches.

-- A merge on an encrypted disk must use its prefetches instead of cancelling them.
-- `ReadBufferFromEncryptedFile` used to forward `prefetch` before applying the pending read range,
-- so the first read applied the range to the inner buffer, which discarded the prefetched data and
-- started a new S3 request.

DROP TABLE IF EXISTS t_encrypted_prefetch;

CREATE TABLE t_encrypted_prefetch
(
    k UInt64,
    s String,
    lc LowCardinality(String),
    n UInt64
)
ENGINE = MergeTree
ORDER BY k
SETTINGS storage_policy = 's3_no_cache_encrypted', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_vertical_merge_algorithm = 0;

INSERT INTO t_encrypted_prefetch SELECT number, concat('val-', toString(cityHash64(number) % 100000)), toString(number % 7), cityHash64(number, 1) FROM numbers(0, 200000);
INSERT INTO t_encrypted_prefetch SELECT number, concat('val-', toString(cityHash64(number) % 100000)), toString(number % 7), cityHash64(number, 1) FROM numbers(200000, 200000);
INSERT INTO t_encrypted_prefetch SELECT number, concat('val-', toString(cityHash64(number) % 100000)), toString(number % 7), cityHash64(number, 1) FROM numbers(400000, 200000);

OPTIMIZE TABLE t_encrypted_prefetch FINAL;

-- The merged data is decrypted correctly.
SELECT count(), sum(k), countDistinct(lc), max(length(s)) FROM t_encrypted_prefetch;

SYSTEM FLUSH LOGS part_log;

-- The merges prefetched, and no prefetch was cancelled.
SELECT
    sum(ProfileEvents['RemoteFSPrefetches']) > 0,
    sum(ProfileEvents['RemoteFSCancelledPrefetches'])
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_encrypted_prefetch' AND event_type = 'MergeParts';

DROP TABLE t_encrypted_prefetch;
