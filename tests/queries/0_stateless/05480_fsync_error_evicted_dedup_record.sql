-- Tags: no-parallel, no-fasttest, no-object-storage
-- Tag no-parallel: the failpoints fail the deferred fsync of all inserts with `fsync_after_insert` and the syncs of all deduplication logs in the server
-- Tag no-fasttest: tests that enable a failpoint run alone, so they are kept out of the fast test
-- Tag no-object-storage: fsync is not applicable to object storage

-- When the fsync of a part fails, the part is detached only after the deduplication log is synced, even if the log has no records
-- of the part: they may have been evicted from the window by newer records, and the eviction is durable only if those records are.
-- An insert without deduplication goes through the same path, because its part has no records in the log either.

DROP TABLE IF EXISTS t_fsync_error_evicted_dedup_record;

CREATE TABLE t_fsync_error_evicted_dedup_record (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS fsync_after_insert = 1, group_fsync_before_insert_finish = 1, non_replicated_deduplication_window = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM ENABLE FAILPOINT parallel_sync_paths_fail_fsync;

-- Syncing the log works, so the part is detached.
INSERT INTO t_fsync_error_evicted_dedup_record SETTINGS insert_deduplicate = 0 VALUES (1, 1), (2, 2); -- { serverError CANNOT_FSYNC }
SELECT count(), sum(v) FROM t_fsync_error_evicted_dedup_record;
SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_fsync_error_evicted_dedup_record';

-- Syncing the log fails, so the part is not detached, although there is nothing to remove from the log.
SYSTEM ENABLE FAILPOINT deduplication_log_fail_durable_drop_sync;
INSERT INTO t_fsync_error_evicted_dedup_record SETTINGS insert_deduplicate = 0 VALUES (1, 1), (2, 2); -- { serverError CANNOT_FSYNC }
SYSTEM DISABLE FAILPOINT deduplication_log_fail_durable_drop_sync;
SYSTEM DISABLE FAILPOINT parallel_sync_paths_fail_fsync;

SELECT count(), sum(v) FROM t_fsync_error_evicted_dedup_record;
SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_fsync_error_evicted_dedup_record';

DROP TABLE t_fsync_error_evicted_dedup_record;
