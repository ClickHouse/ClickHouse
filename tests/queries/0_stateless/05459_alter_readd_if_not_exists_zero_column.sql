-- `ADD COLUMN IF NOT EXISTS` must see a name freed by an earlier command of the same ALTER (#117824).

DROP TABLE IF EXISTS t_readd_after_drop;
CREATE TABLE t_readd_after_drop (x UInt8) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_readd_after_drop DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt8;
DESC TABLE t_readd_after_drop;
DROP TABLE t_readd_after_drop;

DROP TABLE IF EXISTS t_readd_after_rename;
CREATE TABLE t_readd_after_rename (x UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_readd_after_rename VALUES (1);
ALTER TABLE t_readd_after_rename RENAME COLUMN x TO x_old, ADD COLUMN IF NOT EXISTS x UInt8;
DESC TABLE t_readd_after_rename;
SELECT count(), min(x_old), max(x_old) FROM t_readd_after_rename;
DROP TABLE t_readd_after_rename;

DROP TABLE IF EXISTS t_readd_nested;
CREATE TABLE t_readd_nested (n Nested(a UInt8), id UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_readd_nested DROP COLUMN n, ADD COLUMN IF NOT EXISTS n.a UInt8;
DESC TABLE t_readd_nested;
DROP TABLE t_readd_nested;

-- `DROP COLUMN IF EXISTS n` does not remove dotted children, so the following rename of `n.a` still runs.
DROP TABLE IF EXISTS t_readd_nested_skip;
CREATE TABLE t_readd_nested_skip (`n.a` UInt8, id UInt8) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_readd_nested_skip DROP COLUMN IF EXISTS n, RENAME COLUMN IF EXISTS `n.a` TO m;
DESC TABLE t_readd_nested_skip;
DROP TABLE t_readd_nested_skip;

DROP TABLE IF EXISTS t_readd_nested_wide;
CREATE TABLE t_readd_nested_wide (id UInt8, n Nested(a UInt8)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_readd_nested_wide VALUES (1, [7]);
ALTER TABLE t_readd_nested_wide DROP COLUMN n, ADD COLUMN IF NOT EXISTS n Nested(a UInt8) SETTINGS mutations_sync = 2;
SELECT id, `n.a` FROM t_readd_nested_wide;
DROP TABLE t_readd_nested_wide;

DROP TABLE IF EXISTS t_keep_if_exists;
CREATE TABLE t_keep_if_exists (id UInt8, n Nested(a UInt8)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_keep_if_exists VALUES (1, [7]);
ALTER TABLE t_keep_if_exists DROP COLUMN IF EXISTS n SETTINGS mutations_sync = 2;
SELECT id, `n.a` FROM t_keep_if_exists;
DROP TABLE t_keep_if_exists;

DROP TABLE IF EXISTS t_readd_marker;
CREATE TABLE t_readd_marker (id UInt8, `n.a` UInt8)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0,
    skip_empty_columns_on_insert = 1,
    serialization_info_version = 'with_missing_columns',
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_readd_marker VALUES (1, 0);
ALTER TABLE t_readd_marker DROP COLUMN n, ADD COLUMN IF NOT EXISTS `n.a` UInt8 DEFAULT 7 SETTINGS mutations_sync = 2;
SELECT id, `n.a` FROM t_readd_marker;
DROP TABLE t_readd_marker;

DROP TABLE IF EXISTS t_readd_after_rename_nested;
CREATE TABLE t_readd_after_rename_nested (id UInt8, n Nested(a UInt8)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_readd_after_rename_nested VALUES (1, [7]);
ALTER TABLE t_readd_after_rename_nested RENAME COLUMN `n.a` TO `n.b`, DROP COLUMN n, ADD COLUMN IF NOT EXISTS n Nested(b UInt8) SETTINGS mutations_sync = 2;
SELECT id, `n.b` FROM t_readd_after_rename_nested;
DROP TABLE t_readd_after_rename_nested;

DROP TABLE IF EXISTS t_memory_readd;
CREATE TABLE t_memory_readd (x UInt8) ENGINE = Memory;
INSERT INTO t_memory_readd VALUES (7);
ALTER TABLE t_memory_readd DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt8 DEFAULT 9;
SELECT x FROM t_memory_readd;
DROP TABLE t_memory_readd;

DROP TABLE IF EXISTS t_memory_rename_readd;
CREATE TABLE t_memory_rename_readd (x UInt8) ENGINE = Memory;
INSERT INTO t_memory_rename_readd VALUES (7);
ALTER TABLE t_memory_rename_readd RENAME COLUMN x TO x_old, ADD COLUMN IF NOT EXISTS x UInt8 DEFAULT 9;
SELECT x_old, x FROM t_memory_rename_readd;
DROP TABLE t_memory_rename_readd;
