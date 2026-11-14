-- Tags: no-parallel, no-fasttest, no-object-storage
-- Tag no-parallel: the failpoint interrupts the fsync of all inserts with `fsync_after_insert` in the server
-- Tag no-fasttest: tests that enable a failpoint run alone, so they are kept out of the fast test
-- Tag no-object-storage: fsync is not applicable to object storage

-- An interrupted fsync (`EINTR`) is retried. It does not fail the `INSERT`, and the part that a retry
-- of the `INSERT` is deduplicated against is not detached.

DROP TABLE IF EXISTS t_fsync_interrupted;

CREATE TABLE t_fsync_interrupted (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS fsync_after_insert = 1, group_fsync_before_insert_finish = 1, non_replicated_deduplication_window = 100, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM ENABLE FAILPOINT parallel_sync_paths_interrupt_fsync;

INSERT INTO t_fsync_interrupted SETTINGS insert_deduplicate = 1 VALUES (1, 1), (2, 2);
SELECT count(), sum(v) FROM t_fsync_interrupted;

-- The retry is deduplicated against the existing part, which is synced again.
INSERT INTO t_fsync_interrupted SETTINGS insert_deduplicate = 1 VALUES (1, 1), (2, 2);
SELECT count(), sum(v) FROM t_fsync_interrupted;

ALTER TABLE t_fsync_interrupted MODIFY SETTING group_fsync_before_insert_finish = 0;
INSERT INTO t_fsync_interrupted SETTINGS insert_deduplicate = 1 VALUES (1, 1), (2, 2);
SELECT count(), sum(v) FROM t_fsync_interrupted;

SYSTEM DISABLE FAILPOINT parallel_sync_paths_interrupt_fsync;

SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_fsync_interrupted';

DROP TABLE t_fsync_interrupted;
