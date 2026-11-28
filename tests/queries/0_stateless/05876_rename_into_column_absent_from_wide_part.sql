-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: case C needs ReplicatedMergeTree

-- `RENAME COLUMN q TO r, RENAME COLUMN x TO q` in one ALTER, on a part that stores `x` but not `q`.
-- The mutated part must store the data of `x` as `q` and must not list `r`, which has no data in it.

SET mutations_sync = 2, alter_sync = 2;

SELECT 'A: wide part without q';
DROP TABLE IF EXISTS t_absent;
CREATE TABLE t_absent (id UInt64, x UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_absent VALUES (1, 43);
ALTER TABLE t_absent ADD COLUMN q UInt32;
ALTER TABLE t_absent RENAME COLUMN q TO r, RENAME COLUMN x TO q;
SELECT part_type, column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_absent' AND active AND NOT startsWith(column, '_') ORDER BY column_position;
SELECT * FROM t_absent ORDER BY id;
CHECK TABLE t_absent SETTINGS check_query_single_value_result = 1;
DETACH TABLE t_absent;
ATTACH TABLE t_absent;
SELECT * FROM t_absent ORDER BY id;
DROP TABLE t_absent;

SELECT 'B: wide part with q stored only as a missing-column marker';
DROP TABLE IF EXISTS t_marker;
CREATE TABLE t_marker (id UInt64, q UInt32, x UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_marker VALUES (1, 0, 43);
ALTER TABLE t_marker RENAME COLUMN q TO r, RENAME COLUMN x TO q;
SELECT part_type, column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_marker' AND active AND NOT startsWith(column, '_') ORDER BY column_position;
SELECT * FROM t_marker ORDER BY id;
CHECK TABLE t_marker SETTINGS check_query_single_value_result = 1;
DETACH TABLE t_marker;
ATTACH TABLE t_marker;
SELECT * FROM t_marker ORDER BY id;
DROP TABLE t_marker;

SELECT 'C: ReplicatedMergeTree, wide part without q';
DROP TABLE IF EXISTS t_replicated SYNC;
CREATE TABLE t_replicated (id UInt64, x UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_replicated', 'r1') ORDER BY id
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_replicated VALUES (1, 43);
ALTER TABLE t_replicated ADD COLUMN q UInt32;
ALTER TABLE t_replicated RENAME COLUMN q TO r, RENAME COLUMN x TO q;
SELECT part_type, column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_replicated' AND active AND NOT startsWith(column, '_') ORDER BY column_position;
SELECT * FROM t_replicated ORDER BY id;
CHECK TABLE t_replicated SETTINGS check_query_single_value_result = 1;
DETACH TABLE t_replicated;
ATTACH TABLE t_replicated;
SELECT * FROM t_replicated ORDER BY id;
DROP TABLE t_replicated SYNC;

SELECT 'D: compact part without q';
DROP TABLE IF EXISTS t_compact;
CREATE TABLE t_compact (id UInt64, x UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 1000000000;
INSERT INTO t_compact VALUES (1, 43);
ALTER TABLE t_compact ADD COLUMN q UInt32;
ALTER TABLE t_compact RENAME COLUMN q TO r, RENAME COLUMN x TO q;
SELECT part_type, column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_compact' AND active AND NOT startsWith(column, '_') ORDER BY column_position;
SELECT * FROM t_compact ORDER BY id;
CHECK TABLE t_compact SETTINGS check_query_single_value_result = 1;
DROP TABLE t_compact;
