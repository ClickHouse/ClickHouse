-- Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage, no-async-insert, no-random-merge-tree-settings
-- no-fasttest: UNIQUE KEY needs RocksDB.
-- no-object-storage, no-s3-storage: object storage has no fsync, so FileSync stays 0 there.

-- With unique_key_conflict_action = 'ignore', an INSERT whose rows all conflict with existing rows is
-- acknowledged on the strength of those existing rows. With fsync_after_insert = 1 and
-- fsync_after_insert_each_part = 0 it has to sync the existing part that holds them, although it
-- discards its own part. The existing part is written with fsync_after_insert = 0 to make it unsynced.
--
-- The same INSERT into a table that keeps fsync_after_insert = 0 is the baseline.

SET enable_unique_key = 1, insert_deduplicate = 0, async_insert = 0;

DROP TABLE IF EXISTS t_fsync_uk_ignore;
DROP TABLE IF EXISTS t_fsync_uk_ignore_off;

CREATE TABLE t_fsync_uk_ignore (k UInt64, s String) ENGINE = MergeTree UNIQUE KEY (k) ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, unique_key_conflict_action = 'ignore',
    fsync_after_insert = 0, fsync_after_insert_each_part = 0;

CREATE TABLE t_fsync_uk_ignore_off (k UInt64, s String) ENGINE = MergeTree UNIQUE KEY (k) ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, unique_key_conflict_action = 'ignore',
    fsync_after_insert = 0, fsync_after_insert_each_part = 0;

INSERT INTO t_fsync_uk_ignore SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_fsync_uk_ignore_off SELECT number, toString(number) FROM numbers(100);

ALTER TABLE t_fsync_uk_ignore MODIFY SETTING fsync_after_insert = 1;

-- Every row conflicts, so the new part is discarded.
INSERT INTO t_fsync_uk_ignore SETTINGS log_comment = '05453_ignore_on' SELECT number, 'new' FROM numbers(100);
INSERT INTO t_fsync_uk_ignore_off SETTINGS log_comment = '05453_ignore_off' SELECT number, 'new' FROM numbers(100);

SELECT count(), sum(k), countIf(s = 'new') FROM t_fsync_uk_ignore;

SYSTEM FLUSH LOGS query_log;

-- But it syncs the existing part.
SELECT
    (SELECT ProfileEvents['FileSync'] FROM system.query_log
     WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05453_ignore_on')
    >
    (SELECT ProfileEvents['FileSync'] FROM system.query_log
     WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05453_ignore_off');

DROP TABLE t_fsync_uk_ignore;
DROP TABLE t_fsync_uk_ignore_off;
