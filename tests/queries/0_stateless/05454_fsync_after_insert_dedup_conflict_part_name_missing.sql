-- Tags: no-parallel, no-shared-merge-tree, no-fasttest
-- no-parallel: uses a global failpoint in ReplicatedMergeTreeSink::commitPart.
-- no-shared-merge-tree: the failpoint is specific to ReplicatedMergeTreeSink.

-- With fsync_after_insert = 1 a deduplicated block is acknowledged only after the part that already
-- holds it is synced. If the name of that part cannot be resolved (its deduplication node is gone),
-- there is no part to sync, so the INSERT fails instead of acknowledging data that may not be durable.
-- The failpoint forces the "name not resolved" state.

SYSTEM DISABLE FAILPOINT rmt_dedup_conflict_part_name_missing;

DROP TABLE IF EXISTS t_05454 SYNC;

CREATE TABLE t_05454 (k UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05454_dedup/t', 'r1')
ORDER BY k
SETTINGS fsync_after_insert = 1;

INSERT INTO t_05454 SETTINGS async_insert = 0 VALUES (1, 1), (2, 2), (3, 3);

SYSTEM ENABLE FAILPOINT rmt_dedup_conflict_part_name_missing;

INSERT INTO t_05454 SETTINGS async_insert = 0 VALUES (1, 1), (2, 2), (3, 3); -- { serverError UNKNOWN_STATUS_OF_INSERT }

SYSTEM DISABLE FAILPOINT rmt_dedup_conflict_part_name_missing;

-- Without the failpoint the retry is deduplicated as usual.
INSERT INTO t_05454 SETTINGS async_insert = 0 VALUES (1, 1), (2, 2), (3, 3);

SELECT count() FROM t_05454;

DROP TABLE t_05454 SYNC;
