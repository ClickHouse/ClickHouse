-- Tags: zookeeper, no-replicated-database
-- zookeeper: the ReplicatedMergeTree case.
-- no-replicated-database: a Replicated database refuses an ALTER that combines RENAME with UPDATE or DELETE (QUERY_IS_PROHIBITED).

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
SELECT secondary_indices_uncompressed_bytes > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_rename_clear_index' AND active;
DROP TABLE t_rename_clear_index;

DROP TABLE IF EXISTS t_rename_clear_nested;
CREATE TABLE t_rename_clear_nested (k UInt8, n Nested(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_rename_clear_nested VALUES (1, [1, 2], [3, 4]);
ALTER TABLE t_rename_clear_nested RENAME COLUMN n.a TO n.c, CLEAR COLUMN n.c;
SELECT n.b, n.c FROM t_rename_clear_nested;
DROP TABLE t_rename_clear_nested;

DROP TABLE IF EXISTS t_rename_clear_update;
CREATE TABLE t_rename_clear_update (p UInt8, q UInt32, s String) ENGINE = MergeTree ORDER BY p
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_rename_clear_update VALUES (1, 42, 'keep');
ALTER TABLE t_rename_clear_update RENAME COLUMN q TO r, CLEAR COLUMN r, UPDATE s = 'upd' WHERE 1;
SELECT * FROM t_rename_clear_update;
DROP TABLE t_rename_clear_update;

DROP TABLE IF EXISTS t_rename_clear_delete;
CREATE TABLE t_rename_clear_delete (p UInt8, q UInt32) ENGINE = MergeTree ORDER BY p
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_rename_clear_delete VALUES (1, 42), (2, 43);
ALTER TABLE t_rename_clear_delete RENAME COLUMN q TO r, CLEAR COLUMN r, DELETE WHERE p = 2;
SELECT * FROM t_rename_clear_delete;
DROP TABLE t_rename_clear_delete;

DROP TABLE IF EXISTS t_rename_swap_clear;
CREATE TABLE t_rename_swap_clear (k UInt32, a UInt32, c UInt32) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, ratio_of_defaults_for_sparse_serialization = 0.5;
INSERT INTO t_rename_swap_clear SELECT number, number + 1, if(number % 10 = 0, 7, 0) FROM numbers(1000);
ALTER TABLE t_rename_swap_clear RENAME COLUMN a TO b, RENAME COLUMN c TO a, CLEAR COLUMN b;
SELECT 's1-after', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_rename_swap_clear' AND active AND NOT startsWith(column, '_') ORDER BY column;
SELECT 's1-values', sum(a), countIf(a = 7), sum(b), count() FROM t_rename_swap_clear;
CHECK TABLE t_rename_swap_clear SETTINGS check_query_single_value_result = 1;
DROP TABLE t_rename_swap_clear;

DROP TABLE IF EXISTS t_drop_rename_into_sparse;
CREATE TABLE t_drop_rename_into_sparse (k UInt32, a UInt32, c UInt32) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, ratio_of_defaults_for_sparse_serialization = 0.5;
INSERT INTO t_drop_rename_into_sparse SELECT number, number + 1, if(number % 10 = 0, 7, 0) FROM numbers(1000);
ALTER TABLE t_drop_rename_into_sparse DROP COLUMN a, RENAME COLUMN c TO a;
SELECT 's2-after', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_drop_rename_into_sparse' AND active AND NOT startsWith(column, '_') ORDER BY column;
SELECT 's2-values', sum(a), countIf(a = 7), count() FROM t_drop_rename_into_sparse;
CHECK TABLE t_drop_rename_into_sparse SETTINGS check_query_single_value_result = 1;
DROP TABLE t_drop_rename_into_sparse;

DROP TABLE IF EXISTS t_rename_into_cleared;
CREATE TABLE t_rename_into_cleared (k UInt32, a UInt32, c UInt32) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, ratio_of_defaults_for_sparse_serialization = 0.5;
INSERT INTO t_rename_into_cleared SELECT number, if(number % 10 = 0, 5, 0), if(number % 10 = 0, 7, 0) FROM numbers(1000);
ALTER TABLE t_rename_into_cleared RENAME COLUMN a TO b, RENAME COLUMN c TO a, CLEAR COLUMN a;
SELECT 's3-after', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_rename_into_cleared' AND active AND NOT startsWith(column, '_') ORDER BY column;
SELECT 's3-values', sum(a), sum(b), countIf(b = 5), count() FROM t_rename_into_cleared;
CHECK TABLE t_rename_into_cleared SETTINGS check_query_single_value_result = 1;
DROP TABLE t_rename_into_cleared;

DROP TABLE IF EXISTS t_rename_swap_drop;
CREATE TABLE t_rename_swap_drop (k UInt32, a UInt32, c UInt32) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, ratio_of_defaults_for_sparse_serialization = 0.5;
INSERT INTO t_rename_swap_drop SELECT number, if(number % 10 = 0, 5, 0), if(number % 10 = 0, 7, 0) FROM numbers(1000);
ALTER TABLE t_rename_swap_drop RENAME COLUMN a TO b, RENAME COLUMN c TO a, DROP COLUMN a;
SELECT 's4-after', column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_rename_swap_drop' AND active AND NOT startsWith(column, '_') ORDER BY column;
SELECT 's4-values', sum(b), countIf(b = 5), count() FROM t_rename_swap_drop;
CHECK TABLE t_rename_swap_drop SETTINGS check_query_single_value_result = 1;
DROP TABLE t_rename_swap_drop;

DROP TABLE IF EXISTS t_rename_clear_rmt;
CREATE TABLE t_rename_clear_rmt (k UInt8, q UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_rename_clear_rmt', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_rename_clear_rmt VALUES (1, 42);
ALTER TABLE t_rename_clear_rmt RENAME COLUMN q TO r, CLEAR COLUMN r;
SELECT r FROM t_rename_clear_rmt;
DROP TABLE t_rename_clear_rmt;
