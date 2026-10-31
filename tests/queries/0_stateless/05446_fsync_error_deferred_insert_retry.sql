-- Tags: no-parallel, no-fasttest, no-object-storage
-- Tag no-parallel: the failpoint fails the deferred fsync of all inserts with `fsync_after_insert` in the server
-- Tag no-fasttest: tests that enable a failpoint run alone, so they are kept out of the fast test
-- Tag no-object-storage: fsync is not applicable to object storage

-- With `group_fsync_before_insert_finish`, a part is committed before its files are fsynced.
-- If the fsync fails, the data of the part may not be durable, so the part is detached, and it is removed
-- from the deduplication log. A retry of the `INSERT` writes the data again, even after the table is reloaded
-- (as on a restart of the server), instead of being deduplicated against the part that failed to sync.

DROP TABLE IF EXISTS t_fsync_error;

CREATE TABLE t_fsync_error (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS fsync_after_insert = 1, group_fsync_before_insert_finish = 1, non_replicated_deduplication_window = 100, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM ENABLE FAILPOINT parallel_sync_paths_fail_fsync;
INSERT INTO t_fsync_error SETTINGS insert_deduplicate = 1 VALUES (1, 1), (2, 2); -- { serverError CANNOT_FSYNC }
SYSTEM DISABLE FAILPOINT parallel_sync_paths_fail_fsync;

SELECT count(), sum(v) FROM t_fsync_error;
SELECT reason FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_fsync_error';

DETACH TABLE t_fsync_error;
ATTACH TABLE t_fsync_error;

-- The retry is not deduplicated.
INSERT INTO t_fsync_error SETTINGS insert_deduplicate = 1 VALUES (1, 1), (2, 2);
SELECT count(), sum(v) FROM t_fsync_error;

-- The next retry is deduplicated against the new part.
INSERT INTO t_fsync_error SETTINGS insert_deduplicate = 1 VALUES (1, 1), (2, 2);
SELECT count(), sum(v) FROM t_fsync_error;

-- An insert that syncs its own parts inline also detaches the existing part that it is deduplicated against, if it fails to sync.
ALTER TABLE t_fsync_error MODIFY SETTING group_fsync_before_insert_finish = 0;
SYSTEM ENABLE FAILPOINT parallel_sync_paths_fail_fsync;
INSERT INTO t_fsync_error SETTINGS insert_deduplicate = 1 VALUES (1, 1), (2, 2); -- { serverError CANNOT_FSYNC }
SYSTEM DISABLE FAILPOINT parallel_sync_paths_fail_fsync;
SELECT count(), sum(v) FROM t_fsync_error;
SELECT reason, count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_fsync_error' GROUP BY reason;

INSERT INTO t_fsync_error SETTINGS insert_deduplicate = 1 VALUES (1, 1), (2, 2);
SELECT count(), sum(v) FROM t_fsync_error;

DROP TABLE t_fsync_error;
