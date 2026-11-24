-- A part written with `skip_empty_columns_on_insert` omits an all-NULL `Nullable` column. While `MODIFY COLUMN` to a
-- non-`Nullable` type with a `DEFAULT` is pending, its rows read as the `DEFAULT`; its subcolumns, and the functions
-- rewritten to them, must read the same values.

SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_lc;
DROP TABLE IF EXISTS t_default_reads_column;
DROP TABLE IF EXISTS t_remove_default;
DROP TABLE IF EXISTS t_mutation;
DROP TABLE IF EXISTS t_patch_v2;
DROP TABLE IF EXISTS t_patch_v1;

-- Tuple and String, rows 1 and 2 in a part without the columns, row 3 in a part with them.

CREATE TABLE t_compact (c0 UInt64, x Nullable(Tuple(a UInt64, b Nullable(UInt64), arr Array(UInt64), s String, n Tuple(d UInt64))), s Nullable(String))
ENGINE = MergeTree ORDER BY c0
SETTINGS min_bytes_for_wide_part = 10000000000, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    enable_block_number_column = 0, enable_block_offset_column = 0;
SYSTEM STOP MERGES t_compact;
CREATE TABLE t_wide (c0 UInt64, x Nullable(Tuple(a UInt64, b Nullable(UInt64), arr Array(UInt64), s String, n Tuple(d UInt64))), s Nullable(String))
ENGINE = MergeTree ORDER BY c0
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    enable_block_number_column = 0, enable_block_offset_column = 0;
SYSTEM STOP MERGES t_wide;

INSERT INTO t_compact VALUES (1, NULL, NULL), (2, NULL, NULL);
INSERT INTO t_compact VALUES (3, (10, 20, [1], 'zz', (30)), 'q');
INSERT INTO t_wide VALUES (1, NULL, NULL), (2, NULL, NULL);
INSERT INTO t_wide VALUES (3, (10, 20, [1], 'zz', (30)), 'q');

SELECT table, name, part_type, arraySort(groupArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('t_compact', 't_wide') AND active
GROUP BY table, name, part_type ORDER BY table, name;

ALTER TABLE t_compact MODIFY COLUMN x Tuple(a UInt64, b Nullable(UInt64), arr Array(UInt64), s String, n Tuple(d UInt64))
DEFAULT tuple(7, 8, [5, 6], 'def', tuple(9)) SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_compact MODIFY COLUMN s String DEFAULT 'abc' SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_wide MODIFY COLUMN x Tuple(a UInt64, b Nullable(UInt64), arr Array(UInt64), s String, n Tuple(d UInt64))
DEFAULT tuple(7, 8, [5, 6], 'def', tuple(9)) SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_wide MODIFY COLUMN s String DEFAULT 'abc' SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'compact';
SELECT c0, x.a, x.b, x.arr, x.arr.size0, x.s, x.s.size, x.n, x.n.d FROM t_compact ORDER BY c0;
SELECT c0, x, x.a FROM t_compact ORDER BY c0;
SELECT c0, length(s), empty(s) FROM t_compact ORDER BY c0 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT c0, tupleElement(x, 'a') FROM t_compact ORDER BY c0 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT c0 FROM t_compact WHERE x.a = 7 ORDER BY c0;
SELECT c0 FROM t_compact PREWHERE x.a = 7 ORDER BY c0;
SELECT count() FROM t_compact WHERE length(s) = 3 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT c0, x.a, x.n.d FROM t_compact PREWHERE toString(x) != '' ORDER BY c0;
SELECT c0, x.a FROM t_compact ORDER BY c0 LIMIT 1 SETTINGS query_plan_optimize_lazy_materialization = 1;

SELECT 'wide';
SELECT c0, x.a, x.b, x.arr, x.arr.size0, x.s, x.s.size, x.n, x.n.d FROM t_wide ORDER BY c0;
SELECT c0, x, x.a FROM t_wide ORDER BY c0;
SELECT c0, length(s), empty(s) FROM t_wide ORDER BY c0 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT c0, tupleElement(x, 'a') FROM t_wide ORDER BY c0 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT c0 FROM t_wide WHERE x.a = 7 ORDER BY c0;
SELECT c0 FROM t_wide PREWHERE x.a = 7 ORDER BY c0;
SELECT count() FROM t_wide WHERE length(s) = 3 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT c0, x.a, x.n.d FROM t_wide PREWHERE toString(x) != '' ORDER BY c0;
SELECT c0, x.a FROM t_wide ORDER BY c0 LIMIT 1 SETTINGS query_plan_optimize_lazy_materialization = 1;

-- LowCardinality(Nullable(String)).

CREATE TABLE t_lc (c0 UInt64, s LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY c0
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    enable_block_number_column = 0, enable_block_offset_column = 0;
SYSTEM STOP MERGES t_lc;
INSERT INTO t_lc VALUES (1, NULL), (2, NULL);
ALTER TABLE t_lc MODIFY COLUMN s String DEFAULT 'abc' SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'low cardinality';
SELECT c0, s.size FROM t_lc ORDER BY c0;
SELECT c0, length(s), empty(s) FROM t_lc ORDER BY c0 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_lc WHERE length(s) = 3 SETTINGS optimize_functions_to_subcolumns = 1;

-- A DEFAULT that reads another column.

CREATE TABLE t_default_reads_column (c0 UInt64, x Nullable(Tuple(a UInt64, b UInt64))) ENGINE = MergeTree ORDER BY c0
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    enable_block_number_column = 0, enable_block_offset_column = 0;
SYSTEM STOP MERGES t_default_reads_column;
INSERT INTO t_default_reads_column VALUES (1, NULL), (2, NULL);
ALTER TABLE t_default_reads_column MODIFY COLUMN x Tuple(a UInt64, b UInt64) DEFAULT tuple(c0 * 10, c0 * 100)
SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'default reads a column';
SELECT c0, x.a, x.b FROM t_default_reads_column ORDER BY c0;

-- Without a DEFAULT the rows read as the type default.

CREATE TABLE t_remove_default (c0 UInt64, x Nullable(Tuple(a UInt64, b UInt64))) ENGINE = MergeTree ORDER BY c0
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    enable_block_number_column = 0, enable_block_offset_column = 0;
SYSTEM STOP MERGES t_remove_default;
INSERT INTO t_remove_default VALUES (1, NULL);
ALTER TABLE t_remove_default MODIFY COLUMN x Tuple(a UInt64, b UInt64) DEFAULT tuple(7, 8) SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_remove_default MODIFY COLUMN x REMOVE DEFAULT SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'default removed';
SELECT c0, x, x.a FROM t_remove_default ORDER BY c0;
SELECT c0, x.a FROM t_remove_default ORDER BY c0;

-- Mutations that reach the part before the conversion has rewritten it.

CREATE TABLE t_mutation (c0 UInt64, x Nullable(Tuple(a UInt64, b UInt64)), y UInt64 DEFAULT 0) ENGINE = MergeTree ORDER BY c0
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    enable_block_number_column = 0, enable_block_offset_column = 0;
SYSTEM STOP MERGES t_mutation;
INSERT INTO t_mutation (c0, x) VALUES (1, NULL), (2, NULL);
INSERT INTO t_mutation (c0, x) VALUES (3, (0, 0)), (4, (10, 20));
ALTER TABLE t_mutation MODIFY COLUMN x Tuple(a UInt64, b UInt64) DEFAULT tuple(7, 8) SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_mutation UPDATE y = x.b WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_mutation DELETE WHERE x.a = 0 SETTINGS mutations_sync = 0;
SYSTEM START MERGES t_mutation;
ALTER TABLE t_mutation UPDATE y = y WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'mutation';
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_mutation' AND NOT is_done;
SELECT c0, x, y FROM t_mutation ORDER BY c0;

-- A patch of the part without the column.

CREATE TABLE t_patch_v2 (c0 UInt64, x Nullable(Tuple(a UInt64, b UInt64))) ENGINE = MergeTree ORDER BY c0
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';
SYSTEM STOP MERGES t_patch_v2;
INSERT INTO t_patch_v2 VALUES (1, NULL), (2, NULL);
UPDATE t_patch_v2 SET x = (5, 6) WHERE c0 = 2;
ALTER TABLE t_patch_v2 MODIFY COLUMN x Tuple(a UInt64, b UInt64) DEFAULT tuple(7, 8) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'patches v2';
SELECT c0, x, x.a, x.b FROM t_patch_v2 ORDER BY c0;
SELECT c0, x.a FROM t_patch_v2 ORDER BY c0;

CREATE TABLE t_patch_v1 (c0 UInt64, x Nullable(Tuple(a UInt64, b UInt64))) ENGINE = MergeTree ORDER BY c0
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v1';
SYSTEM STOP MERGES t_patch_v1;
INSERT INTO t_patch_v1 VALUES (1, NULL), (2, NULL);
UPDATE t_patch_v1 SET x = (5, 6) WHERE c0 = 2;
ALTER TABLE t_patch_v1 MODIFY COLUMN x Tuple(a UInt64, b UInt64) DEFAULT tuple(7, 8) SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'patches v1';
SELECT c0, x, x.a, x.b FROM t_patch_v1 ORDER BY c0;
SELECT c0, x.a FROM t_patch_v1 ORDER BY c0;

DROP TABLE t_compact;
DROP TABLE t_wide;
DROP TABLE t_lc;
DROP TABLE t_default_reads_column;
DROP TABLE t_remove_default;
DROP TABLE t_mutation;
DROP TABLE t_patch_v2;
DROP TABLE t_patch_v1;
