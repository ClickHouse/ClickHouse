-- Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage, no-async-insert, no-random-merge-tree-settings
-- no-fasttest: UNIQUE KEY needs RocksDB.
-- no-object-storage, no-s3-storage: object storage has no fsync, so FileSync stays 0 there.

-- The same as 05394_fsync_after_insert_batched_deduplicated, but for a UNIQUE KEY table, which commits
-- its parts in a different way: with fsync_after_insert = 1 and fsync_after_insert_each_part = 0 an
-- INSERT whose block is deduplicated has to sync the existing part that holds the block. The existing
-- part is written with fsync_after_insert = 0 to make it unsynced.
--
-- `txn_version.txt` is synced when it is written regardless of `fsync_after_insert`, so the same
-- deduplicated INSERT into a table that keeps fsync_after_insert = 0 is the baseline.

SET enable_unique_key = 1, insert_deduplicate = 1, async_insert = 0;

DROP TABLE IF EXISTS t_fsync_dedup_uk;
DROP TABLE IF EXISTS t_fsync_dedup_uk_off;

CREATE TABLE t_fsync_dedup_uk (k UInt64, s String) ENGINE = MergeTree UNIQUE KEY (k) ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, non_replicated_deduplication_window = 100,
    fsync_after_insert = 0, fsync_after_insert_each_part = 0;

CREATE TABLE t_fsync_dedup_uk_off (k UInt64, s String) ENGINE = MergeTree UNIQUE KEY (k) ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, non_replicated_deduplication_window = 100,
    fsync_after_insert = 0, fsync_after_insert_each_part = 0;

INSERT INTO t_fsync_dedup_uk SETTINGS insert_deduplication_token = 'token' SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_fsync_dedup_uk_off SETTINGS insert_deduplication_token = 'token' SELECT number, toString(number) FROM numbers(100);

ALTER TABLE t_fsync_dedup_uk MODIFY SETTING fsync_after_insert = 1;

INSERT INTO t_fsync_dedup_uk SETTINGS insert_deduplication_token = 'token', log_comment = '05406_dedup_on' SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_fsync_dedup_uk_off SETTINGS insert_deduplication_token = 'token', log_comment = '05406_dedup_off' SELECT number, toString(number) FROM numbers(100);

-- The second INSERT is deduplicated.
SELECT count(), sum(k) FROM t_fsync_dedup_uk;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['DuplicatedInsertedBlocks'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05406_dedup_%'
ORDER BY log_comment;

-- But it syncs the existing part.
SELECT
    (SELECT ProfileEvents['FileSync'] FROM system.query_log
     WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05406_dedup_on')
    >
    (SELECT ProfileEvents['FileSync'] FROM system.query_log
     WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05406_dedup_off');

DROP TABLE t_fsync_dedup_uk;
DROP TABLE t_fsync_dedup_uk_off;
