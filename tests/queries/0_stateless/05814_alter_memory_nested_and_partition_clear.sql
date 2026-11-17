-- A recreated column's default reads a subcolumn of another column recreated in the same ALTER.
DROP TABLE IF EXISTS t_memory_tuple_subcolumn;
CREATE TABLE t_memory_tuple_subcolumn (id UInt8, a Tuple(v UInt64), x UInt64) ENGINE = Memory;
INSERT INTO t_memory_tuple_subcolumn VALUES (1, (7), 0);
ALTER TABLE t_memory_tuple_subcolumn
    DROP COLUMN a, ADD COLUMN IF NOT EXISTS a Tuple(v UInt64) DEFAULT tuple(9),
    DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT a.v + 1;
SELECT id, a, x FROM t_memory_tuple_subcolumn;
DROP TABLE t_memory_tuple_subcolumn;

-- Clearing one Nested member keeps the sibling's array sizes.
DROP TABLE IF EXISTS t_memory_nested_clear;
CREATE TABLE t_memory_nested_clear (id UInt8, n Nested(a UInt8, b UInt8)) ENGINE = Memory;
INSERT INTO t_memory_nested_clear VALUES (1, [7, 8], [9, 10]);
ALTER TABLE t_memory_nested_clear CLEAR COLUMN `n.a`;
SELECT n.a, n.b FROM t_memory_nested_clear;
SELECT n.a, n.b FROM t_memory_nested_clear ARRAY JOIN n.a, n.b;
DROP TABLE t_memory_nested_clear;

-- A missing Nested member is read with the sibling's sizes. A replacement default must store that value.
DROP TABLE IF EXISTS t_memory_nested_missing;
CREATE TABLE t_memory_nested_missing (id UInt8, `n.a` Array(UInt8), x UInt64) ENGINE = Memory;
INSERT INTO t_memory_nested_missing VALUES (1, [7, 8], 0);
ALTER TABLE t_memory_nested_missing ADD COLUMN `n.b` Array(UInt8);
SELECT n.b, length(n.b) + 1 FROM t_memory_nested_missing;
ALTER TABLE t_memory_nested_missing DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT length(n.b) + 1;
SELECT n.b, x, length(n.b) + 1 FROM t_memory_nested_missing;
DROP TABLE t_memory_nested_missing;

-- A column added after the block was written is the type default, not its expression, for a later replacement.
DROP TABLE IF EXISTS t_memory_missing_default;
CREATE TABLE t_memory_missing_default (id UInt8, x UInt64) ENGINE = Memory;
INSERT INTO t_memory_missing_default VALUES (1, 7);
ALTER TABLE t_memory_missing_default ADD COLUMN a UInt64 DEFAULT 257;
SELECT a, a + 1 FROM t_memory_missing_default;
ALTER TABLE t_memory_missing_default DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT a + 1;
SELECT a, x, a + 1 FROM t_memory_missing_default;
DROP TABLE t_memory_missing_default;

-- An unrelated lazy type change does not block a constant clear.
DROP TABLE IF EXISTS t_memory_unrelated_type;
CREATE TABLE t_memory_unrelated_type (a String, x UInt64) ENGINE = Memory;
INSERT INTO t_memory_unrelated_type VALUES ('bad', 7);
ALTER TABLE t_memory_unrelated_type MODIFY COLUMN a UInt8;
SELECT x FROM t_memory_unrelated_type;
ALTER TABLE t_memory_unrelated_type CLEAR COLUMN x;
SELECT x FROM t_memory_unrelated_type;
DROP TABLE t_memory_unrelated_type;

-- `DROP COLUMN IF EXISTS` of a Nested parent that is not an exact column keeps the child.
DROP TABLE IF EXISTS t_if_exists_nested;
CREATE TABLE t_if_exists_nested (id UInt8, n Nested(a UInt8)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_if_exists_nested DROP COLUMN IF EXISTS n, COMMENT COLUMN `n.a` 'retained';
SELECT comment FROM system.columns WHERE database = currentDatabase() AND table = 't_if_exists_nested' AND name = 'n.a';
DROP TABLE t_if_exists_nested;

-- A type change in the same statement is visible to a later subcolumn codec.
SET enable_subcolumn_codecs = 1;
DROP TABLE IF EXISTS t_subcolumn_after_type;
CREATE TABLE t_subcolumn_after_type (id UInt8, a Tuple(v UInt64)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_subcolumn_after_type
    MODIFY COLUMN a Tuple(v UInt64, w UInt64),
    MODIFY COLUMN IF EXISTS a.w CODEC(ZSTD);
SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name = 't_subcolumn_after_type'
    AND create_table_query LIKE '%`a.w` (ZSTD%';
DROP TABLE t_subcolumn_after_type;

-- `CLEAR` in one partition does not drop a lightweight update stored in another partition.
SET enable_lightweight_update = 1, mutations_sync = 0, alter_sync = 0;
DROP TABLE IF EXISTS t_clear_other_partition;
CREATE TABLE t_clear_other_partition (p UInt8, id UInt8, x UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_clear_other_partition VALUES (1, 1, 7), (2, 2, 8);
SYSTEM STOP MERGES t_clear_other_partition;
UPDATE t_clear_other_partition SET x = 42 WHERE p = 2;
ALTER TABLE t_clear_other_partition CLEAR COLUMN x IN PARTITION 1;
SELECT p, x FROM t_clear_other_partition ORDER BY p;
SYSTEM START MERGES t_clear_other_partition;
ALTER TABLE t_clear_other_partition DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT p, x FROM t_clear_other_partition ORDER BY p;
DROP TABLE t_clear_other_partition;
