-- #117824: `ADD COLUMN IF NOT EXISTS` sees a name freed earlier in the same `ALTER`.

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

-- `DROP COLUMN IF EXISTS n` leaves `n.a`, so the rename still runs.
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

DROP TABLE IF EXISTS t_memory_type_default;
CREATE TABLE t_memory_type_default (x UInt8) ENGINE = Memory;
INSERT INTO t_memory_type_default VALUES (7);
ALTER TABLE t_memory_type_default DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt8;
SELECT x FROM t_memory_type_default;
SELECT count() FROM t_memory_type_default SETTINGS optimize_trivial_count_query = 0;
DROP TABLE t_memory_type_default;

DROP TABLE IF EXISTS t_memory_clear;
CREATE TABLE t_memory_clear (x UInt8) ENGINE = Memory;
INSERT INTO t_memory_clear VALUES (7);
ALTER TABLE t_memory_clear CLEAR COLUMN x;
SELECT x FROM t_memory_clear;
SELECT count() FROM t_memory_clear SETTINGS optimize_trivial_count_query = 0;
DROP TABLE t_memory_clear;

DROP TABLE IF EXISTS t_memory_exact;
CREATE TABLE t_memory_exact (n UInt8, `n.a` UInt8, id UInt8) ENGINE = Memory;
INSERT INTO t_memory_exact VALUES (7, 9, 1);
ALTER TABLE t_memory_exact DROP COLUMN n;
SELECT `n.a`, id FROM t_memory_exact;
DROP TABLE t_memory_exact;

DROP TABLE IF EXISTS t_memory_rename_exact;
CREATE TABLE t_memory_rename_exact (n UInt8, `n.a` UInt8, id UInt8) ENGINE = Memory;
INSERT INTO t_memory_rename_exact VALUES (7, 9, 1);
ALTER TABLE t_memory_rename_exact RENAME COLUMN n TO m;
SELECT m, `n.a`, id FROM t_memory_rename_exact;
DROP TABLE t_memory_rename_exact;

DROP TABLE IF EXISTS t_memory_compress;
CREATE TABLE t_memory_compress (a UInt64, x UInt64) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO t_memory_compress SELECT number, 7 FROM numbers(1000);
ALTER TABLE t_memory_compress DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT a + 1;
SELECT min(a), min(x), max(x), count() FROM t_memory_compress;
DROP TABLE t_memory_compress;

DROP TABLE IF EXISTS t_readd_scalar_wide;
CREATE TABLE t_readd_scalar_wide (id UInt8, a UInt64) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_readd_scalar_wide VALUES (1, 7);
ALTER TABLE t_readd_scalar_wide RENAME COLUMN a TO b, DROP COLUMN b, ADD COLUMN IF NOT EXISTS b UInt64 DEFAULT 9 SETTINGS mutations_sync = 2;
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_readd_scalar_wide' AND active;
SELECT id, b FROM t_readd_scalar_wide;
DROP TABLE t_readd_scalar_wide;

DROP TABLE IF EXISTS t_readd_mixed_marker;
CREATE TABLE t_readd_mixed_marker (id UInt8, `n.a` UInt8, `n.b` UInt8)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 1000000, min_bytes_for_wide_part = 1000000000,
    ratio_of_defaults_for_sparse_serialization = 1.0,
    skip_empty_columns_on_insert = 1,
    serialization_info_version = 'with_missing_columns',
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_readd_mixed_marker VALUES (1, 5, 0);
ALTER TABLE t_readd_mixed_marker DROP COLUMN n, ADD COLUMN IF NOT EXISTS `n.a` UInt8 DEFAULT 7, ADD COLUMN IF NOT EXISTS `n.b` UInt8 DEFAULT 9 SETTINGS mutations_sync = 2;
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_readd_mixed_marker' AND active;
SELECT id, `n.a`, `n.b` FROM t_readd_mixed_marker;
DROP TABLE t_readd_mixed_marker;

-- A rewrite must not apply to inserts created after the ALTER.
DROP TABLE IF EXISTS t_memory_clear_then_insert;
CREATE TABLE t_memory_clear_then_insert (x UInt8) ENGINE = Memory;
INSERT INTO t_memory_clear_then_insert VALUES (7);
ALTER TABLE t_memory_clear_then_insert CLEAR COLUMN x;
INSERT INTO t_memory_clear_then_insert VALUES (42);
SELECT x FROM t_memory_clear_then_insert ORDER BY x;
DROP TABLE t_memory_clear_then_insert;

DROP TABLE IF EXISTS t_memory_readd_then_insert;
CREATE TABLE t_memory_readd_then_insert (x UInt8) ENGINE = Memory;
INSERT INTO t_memory_readd_then_insert VALUES (7);
ALTER TABLE t_memory_readd_then_insert DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt8 DEFAULT 9;
INSERT INTO t_memory_readd_then_insert VALUES (42);
SELECT x FROM t_memory_readd_then_insert ORDER BY x;
DROP TABLE t_memory_readd_then_insert;

-- The column that remains was never stored. The row still has to survive.
DROP TABLE IF EXISTS t_memory_drop_last;
CREATE TABLE t_memory_drop_last (x UInt8) ENGINE = Memory;
INSERT INTO t_memory_drop_last VALUES (1);
ALTER TABLE t_memory_drop_last ADD COLUMN y UInt8;
ALTER TABLE t_memory_drop_last DROP COLUMN x;
SELECT count() FROM t_memory_drop_last SETTINGS optimize_trivial_count_query = 0;
SELECT y FROM t_memory_drop_last;
DROP TABLE t_memory_drop_last;

DROP TABLE IF EXISTS t_memory_clear_nested;
CREATE TABLE t_memory_clear_nested (n Nested(a UInt8, b UInt8)) ENGINE = Memory;
INSERT INTO t_memory_clear_nested VALUES ([1], [2]);
ALTER TABLE t_memory_clear_nested CLEAR COLUMN n;
SELECT count() FROM t_memory_clear_nested SETTINGS optimize_trivial_count_query = 0;
SELECT n.a, n.b FROM t_memory_clear_nested;
DROP TABLE t_memory_clear_nested;

-- A failed ALTER must not stay in the rewrite history.
DROP TABLE IF EXISTS t_memory_failed_alter;
CREATE TABLE t_memory_failed_alter (a UInt64, x UInt64) ENGINE = Memory;
INSERT INTO t_memory_failed_alter VALUES (0, 7);
ALTER TABLE t_memory_failed_alter DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT intDiv(100, a); -- { serverError ILLEGAL_DIVISION }
SELECT default_expression = '' FROM system.columns WHERE database = currentDatabase() AND table = 't_memory_failed_alter' AND name = 'x';
INSERT INTO t_memory_failed_alter VALUES (1, 42);
SELECT a, x FROM t_memory_failed_alter ORDER BY a;
DROP TABLE t_memory_failed_alter;

-- Exact `n` (here only a missing-column marker) must not delete stored `n.a`.
DROP TABLE IF EXISTS t_wide_exact_over_dotted;
CREATE TABLE t_wide_exact_over_dotted (id UInt8, n UInt8, `n.a` UInt8)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0,
    skip_empty_columns_on_insert = 1,
    serialization_info_version = 'with_missing_columns',
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_wide_exact_over_dotted VALUES (1, 0, 9);
ALTER TABLE t_wide_exact_over_dotted DROP COLUMN n SETTINGS mutations_sync = 2;
SELECT id, `n.a` FROM t_wide_exact_over_dotted;
DROP TABLE t_wide_exact_over_dotted;

-- Exact stored `n` must not rewrite independent `n.a` from a new default.
DROP TABLE IF EXISTS t_compact_exact_over_marker;
CREATE TABLE t_compact_exact_over_marker (id UInt8, n UInt8, `n.a` UInt8 DEFAULT 0)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_wide_part = 1000000, min_bytes_for_wide_part = 1000000000,
    ratio_of_defaults_for_sparse_serialization = 1.0,
    skip_empty_columns_on_insert = 1,
    serialization_info_version = 'with_missing_columns',
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_compact_exact_over_marker (id, n) VALUES (1, 7);
ALTER TABLE t_compact_exact_over_marker DROP COLUMN n, MODIFY COLUMN `n.a` DEFAULT 5 SETTINGS mutations_sync = 2;
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact_exact_over_marker' AND active;
SELECT id, `n.a` FROM t_compact_exact_over_marker;
DROP TABLE t_compact_exact_over_marker;

-- Re-adding `x` and renaming it in one statement keeps the replacement default.
DROP TABLE IF EXISTS t_memory_readd_rename;
CREATE TABLE t_memory_readd_rename (id UInt8, x UInt8) ENGINE = Memory;
INSERT INTO t_memory_readd_rename VALUES (1, 7);
ALTER TABLE t_memory_readd_rename DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt8 DEFAULT 9, RENAME COLUMN x TO y;
SELECT id, y FROM t_memory_readd_rename;
DROP TABLE t_memory_readd_rename;

-- A default that closes a dependency cycle must not change stored rows or later inserts.
DROP DICTIONARY IF EXISTS d_memory_cycle;
DROP TABLE IF EXISTS t_memory_cycle;
CREATE TABLE t_memory_cycle (id UInt64, x UInt8) ENGINE = Memory;
INSERT INTO t_memory_cycle VALUES (1, 7);
CREATE DICTIONARY d_memory_cycle (id UInt64, x UInt8)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 't_memory_cycle' DB currentDatabase()))
LAYOUT(FLAT())
LIFETIME(0);
ALTER TABLE t_memory_cycle DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt8 DEFAULT dictGetUInt8(concat(currentDatabase(), '.d_memory_cycle'), 'x', id) + 1; -- { serverError INFINITE_LOOP }
SELECT x FROM t_memory_cycle;
INSERT INTO t_memory_cycle VALUES (2, 42);
SELECT x FROM t_memory_cycle ORDER BY id;
SELECT default_expression = '' FROM system.columns WHERE database = currentDatabase() AND table = 't_memory_cycle' AND name = 'x';
DROP DICTIONARY d_memory_cycle;
DROP TABLE t_memory_cycle;

-- Rewriting a temporary table checks `max_temporary_table_memory_usage` before publishing.
SET max_temporary_table_memory_usage = 4096;
CREATE TEMPORARY TABLE t_memory_temp_limit (x UInt8) ENGINE = Memory;
INSERT INTO t_memory_temp_limit VALUES (7);
ALTER TABLE t_memory_temp_limit DROP COLUMN x, ADD COLUMN IF NOT EXISTS x String DEFAULT repeat('a', 100000); -- { serverError TOO_MANY_BYTES }
SELECT x FROM t_memory_temp_limit;
