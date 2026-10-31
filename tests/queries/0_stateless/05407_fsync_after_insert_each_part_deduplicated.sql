-- Tags: no-object-storage, no-random-merge-tree-settings
-- no-object-storage: object storage has no fsync, so FileSync stays 0 there.

-- An INSERT whose block is deduplicated is acknowledged on the strength of the part that already
-- holds the block, which may never have been synced (e.g. the INSERT that committed it in the
-- batched mode was cancelled before its fsync at the end of the query). The deduplicated INSERT has
-- to sync that part also with fsync_after_insert_each_part = 1, where it syncs its own part inline,
-- because its own part is discarded. Here the existing part is written with fsync_after_insert = 0
-- to make it unsynced.
--
-- To tell the sync of the existing part from the inline sync of the discarded part, the
-- deduplicated INSERT is compared with the same INSERT of a new block: it has to sync more files.

SET insert_deduplicate = 1, async_insert = 0;

DROP TABLE IF EXISTS t_fsync_dedup_each_part;
DROP TABLE IF EXISTS t_fsync_dedup_each_part_replicated;

CREATE TABLE t_fsync_dedup_each_part (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, non_replicated_deduplication_window = 100,
    fsync_after_insert = 0, fsync_after_insert_each_part = 1;

CREATE TABLE t_fsync_dedup_each_part_replicated (k UInt64, s String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_fsync_dedup_each_part_replicated', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, fsync_after_insert = 0, fsync_after_insert_each_part = 1;

INSERT INTO t_fsync_dedup_each_part SETTINGS insert_deduplication_token = 'token' SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_fsync_dedup_each_part_replicated SETTINGS insert_deduplication_token = 'token' SELECT number, toString(number) FROM numbers(100);

ALTER TABLE t_fsync_dedup_each_part MODIFY SETTING fsync_after_insert = 1;
ALTER TABLE t_fsync_dedup_each_part_replicated MODIFY SETTING fsync_after_insert = 1;

INSERT INTO t_fsync_dedup_each_part SETTINGS insert_deduplication_token = 'token', log_comment = 'fsync_each_part_dedup' SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_fsync_dedup_each_part_replicated SETTINGS insert_deduplication_token = 'token', log_comment = 'fsync_each_part_dedup_replicated' SELECT number, toString(number) FROM numbers(100);

INSERT INTO t_fsync_dedup_each_part SETTINGS insert_deduplication_token = 'other', log_comment = 'fsync_each_part_new' SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_fsync_dedup_each_part_replicated SETTINGS insert_deduplication_token = 'other', log_comment = 'fsync_each_part_new_replicated' SELECT number, toString(number) FROM numbers(100);

-- The first retry is deduplicated, the INSERT of the new block is not.
SELECT count(), sum(k) FROM t_fsync_dedup_each_part;
SELECT count(), sum(k) FROM t_fsync_dedup_each_part_replicated;

SYSTEM FLUSH LOGS query_log;

SELECT
    replaceRegexpOne(log_comment, '^fsync_each_part_(dedup|new)', '') AS engine,
    anyIf(ProfileEvents['DuplicatedInsertedBlocks'], log_comment LIKE 'fsync_each_part_dedup%') > 0,
    anyIf(ProfileEvents['FileSync'], log_comment LIKE 'fsync_each_part_dedup%') > anyIf(ProfileEvents['FileSync'], log_comment LIKE 'fsync_each_part_new%')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'fsync_each_part_%'
GROUP BY engine
ORDER BY engine;

DROP TABLE t_fsync_dedup_each_part;
DROP TABLE t_fsync_dedup_each_part_replicated;
