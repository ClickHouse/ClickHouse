-- Tags: no-parallel, no-fasttest, no-object-storage
-- Tag no-parallel: the failpoint fails the deferred fsync of all inserts with `fsync_after_insert` in the server
-- Tag no-fasttest: tests that enable a failpoint run alone, so they are kept out of the fast test
-- Tag no-object-storage: fsync is not applicable to object storage

-- With `group_fsync_before_insert_finish`, a part is committed before its files are fsynced.
-- If the fsync fails, a retry of the `INSERT` that is deduplicated against this part must fail as well,
-- even after a mutation that hardlinks the files of this part, because the kernel may report success
-- for the fsync of the same file through a new file descriptor, although the data was not written.

DROP TABLE IF EXISTS t_fsync_error;

CREATE TABLE t_fsync_error (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS fsync_after_insert = 1, group_fsync_before_insert_finish = 1, non_replicated_deduplication_window = 100, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM ENABLE FAILPOINT parallel_sync_paths_fail_fsync;
INSERT INTO t_fsync_error SETTINGS insert_deduplicate = 1 VALUES (1, 1), (2, 2); -- { serverError CANNOT_FSYNC }
SYSTEM DISABLE FAILPOINT parallel_sync_paths_fail_fsync;

-- The part is committed, but its data may not be durable.
SELECT count(), sum(v) FROM t_fsync_error;

-- The retry is deduplicated against the part that failed to sync.
INSERT INTO t_fsync_error SETTINGS insert_deduplicate = 1 VALUES (1, 1), (2, 2); -- { serverError CANNOT_FSYNC }

-- The mutation rewrites the column `v`, but hardlinks the files of the column `k`.
ALTER TABLE t_fsync_error UPDATE v = v + 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT count(), sum(v) FROM t_fsync_error;

INSERT INTO t_fsync_error SETTINGS insert_deduplicate = 1 VALUES (1, 1), (2, 2); -- { serverError CANNOT_FSYNC }

-- Other inserts are not affected.
INSERT INTO t_fsync_error SETTINGS insert_deduplicate = 1 VALUES (3, 3);
SELECT count(), sum(v) FROM t_fsync_error;

DROP TABLE t_fsync_error;
