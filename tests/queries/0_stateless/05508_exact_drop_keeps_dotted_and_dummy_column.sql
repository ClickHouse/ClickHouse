-- A user column named `_dummy` is not the row-count placeholder.
DROP TABLE IF EXISTS t_dummy_kept;
CREATE TABLE t_dummy_kept (_dummy UInt8, x UInt8) ENGINE = Memory;
INSERT INTO t_dummy_kept VALUES (7, 8);
ALTER TABLE t_dummy_kept DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt8 DEFAULT 9;
SELECT _dummy, x FROM t_dummy_kept;
DROP TABLE t_dummy_kept;

-- The placeholder must not replace a re-added column of the same name.
DROP TABLE IF EXISTS t_dummy_readded;
CREATE TABLE t_dummy_readded (x UInt8) ENGINE = Memory;
INSERT INTO t_dummy_readded VALUES (7);
ALTER TABLE t_dummy_readded DROP COLUMN x, ADD COLUMN IF NOT EXISTS _dummy UInt8 DEFAULT 9;
SELECT _dummy FROM t_dummy_readded;
DROP TABLE t_dummy_readded;

-- Rename and drop share one Nested offsets stream. The sibling array stays.
DROP TABLE IF EXISTS t_nested_sibling;
CREATE TABLE t_nested_sibling (id UInt8, n Nested(a UInt8, c UInt8))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_nested_sibling VALUES (1, [7, 8], [9, 10]);
ALTER TABLE t_nested_sibling RENAME COLUMN `n.a` TO `n.b`, DROP COLUMN `n.b`, ADD COLUMN IF NOT EXISTS `n.b` Array(UInt8) SETTINGS mutations_sync = 2;
SELECT n.c FROM t_nested_sibling;
DROP TABLE t_nested_sibling;

-- Exact `n` was an alias, so the part never stored it and has no missing-column marker.
DROP TABLE IF EXISTS t_exact_memory;
CREATE TABLE t_exact_memory (id UInt8, n UInt8 ALIAS id, `n.a` UInt8) ENGINE = Memory;
INSERT INTO t_exact_memory (id, `n.a`) VALUES (1, 9);
ALTER TABLE t_exact_memory MODIFY COLUMN n REMOVE ALIAS;
ALTER TABLE t_exact_memory DROP COLUMN n;
SELECT id, `n.a` FROM t_exact_memory;
DROP TABLE t_exact_memory;

DROP TABLE IF EXISTS t_exact_wide;
CREATE TABLE t_exact_wide (id UInt8, n UInt8 ALIAS id, `n.a` UInt8)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_exact_wide (id, `n.a`) VALUES (1, 9);
ALTER TABLE t_exact_wide MODIFY COLUMN n REMOVE ALIAS;
ALTER TABLE t_exact_wide DROP COLUMN n SETTINGS mutations_sync = 2;
SELECT id, `n.a` FROM t_exact_wide;
DROP TABLE t_exact_wide;

DROP TABLE IF EXISTS t_exact_compact;
CREATE TABLE t_exact_compact (id UInt8, n UInt8 ALIAS id, `n.a` UInt8)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 1000000, min_bytes_for_wide_part = 1000000000,
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_exact_compact (id, `n.a`) VALUES (1, 9);
ALTER TABLE t_exact_compact MODIFY COLUMN n REMOVE ALIAS;
ALTER TABLE t_exact_compact DROP COLUMN n SETTINGS mutations_sync = 2;
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_exact_compact' AND active;
SELECT id, `n.a` FROM t_exact_compact;
DROP TABLE t_exact_compact;
