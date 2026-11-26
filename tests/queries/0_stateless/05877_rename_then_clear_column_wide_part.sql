-- Tags: zookeeper
-- zookeeper: the ReplicatedMergeTree case.

-- A CLEAR COLUMN of a column renamed earlier in the same ALTER kept its data on a wide part.

SET mutations_sync = 2, alter_sync = 2;

DROP TABLE IF EXISTS t_rename_clear;
CREATE TABLE t_rename_clear (p UInt8, q UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY p
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_rename_clear VALUES (1, 11), (2, 22);
SELECT DISTINCT part_type, part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 't_rename_clear' AND active;

ALTER TABLE t_rename_clear RENAME COLUMN q TO r, CLEAR COLUMN r IN PARTITION 1;
SELECT p, r FROM t_rename_clear ORDER BY p;

ALTER TABLE t_rename_clear RENAME COLUMN r TO s, CLEAR COLUMN s;
SELECT p, s FROM t_rename_clear ORDER BY p;
SELECT partition, groupArray(column) FROM (SELECT partition, column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_rename_clear' AND active AND NOT startsWith(column, '_') ORDER BY column) GROUP BY partition ORDER BY partition;

DETACH TABLE t_rename_clear;
ATTACH TABLE t_rename_clear;
SELECT p, s FROM t_rename_clear ORDER BY p;
CHECK TABLE t_rename_clear SETTINGS check_query_single_value_result = 1;
DROP TABLE t_rename_clear;

DROP TABLE IF EXISTS t_rename_clear_index;
CREATE TABLE t_rename_clear_index (k UInt8, q UInt32, INDEX i q TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_rename_clear_index VALUES (1, 42);
ALTER TABLE t_rename_clear_index RENAME COLUMN q TO r, CLEAR COLUMN r;
SELECT r, (SELECT count() FROM t_rename_clear_index WHERE r = 0) FROM t_rename_clear_index;
DROP TABLE t_rename_clear_index;

DROP TABLE IF EXISTS t_rename_clear_nested;
CREATE TABLE t_rename_clear_nested (k UInt8, n Nested(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_rename_clear_nested VALUES (1, [1, 2], [3, 4]);
ALTER TABLE t_rename_clear_nested RENAME COLUMN n.a TO n.c, CLEAR COLUMN n.c;
SELECT n.b, n.c FROM t_rename_clear_nested;
DROP TABLE t_rename_clear_nested;

DROP TABLE IF EXISTS t_rename_clear_rmt;
CREATE TABLE t_rename_clear_rmt (k UInt8, q UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_rename_clear_rmt', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_rename_clear_rmt VALUES (1, 42);
ALTER TABLE t_rename_clear_rmt RENAME COLUMN q TO r, CLEAR COLUMN r;
SELECT r FROM t_rename_clear_rmt;
DROP TABLE t_rename_clear_rmt;
