-- Tags: no-parallel, no-fasttest, no-object-storage
-- Tag no-parallel: the failpoints fail the deferred fsync of all inserts with `fsync_after_insert` and the syncs of all deduplication logs in the server
-- Tag no-fasttest: tests that enable a failpoint run alone, so they are kept out of the fast test
-- Tag no-object-storage: fsync is not applicable to object storage

-- When the fsync of a part fails, the part is removed from the deduplication log, and the removal is synced before the part is detached.
-- If syncing the removal fails, the part is not detached. The removal is not known to be durable then, and syncing the log again
-- would not prove otherwise, so later attempts to detach a part that failed to sync must not detach it either.

DROP TABLE IF EXISTS t_fsync_error_dedup_log;

CREATE TABLE t_fsync_error_dedup_log (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS fsync_after_insert = 1, group_fsync_before_insert_finish = 1, non_replicated_deduplication_window = 100, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM ENABLE FAILPOINT parallel_sync_paths_fail_fsync;
SYSTEM ENABLE FAILPOINT deduplication_log_fail_durable_drop_sync;
INSERT INTO t_fsync_error_dedup_log SETTINGS insert_deduplicate = 1 VALUES (1, 1), (2, 2); -- { serverError CANNOT_FSYNC }
SYSTEM DISABLE FAILPOINT deduplication_log_fail_durable_drop_sync;

SELECT count(), sum(v) FROM t_fsync_error_dedup_log;
SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_fsync_error_dedup_log';

-- The retry is not deduplicated, because the block was already removed from the deduplication log in memory.
-- Its new part fails to sync too, and it is not detached either, even though syncing the deduplication log works now.
INSERT INTO t_fsync_error_dedup_log SETTINGS insert_deduplicate = 1 VALUES (1, 1), (2, 2); -- { serverError CANNOT_FSYNC }
SYSTEM DISABLE FAILPOINT parallel_sync_paths_fail_fsync;

SELECT count(), sum(v) FROM t_fsync_error_dedup_log;
SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_fsync_error_dedup_log';

DROP TABLE t_fsync_error_dedup_log;
