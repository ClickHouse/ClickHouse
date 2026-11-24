-- A part written with `skip_empty_columns_on_insert` omits an all-default column and keeps a marker with its write-time
-- type. While a type-changing `MODIFY COLUMN` is pending, a subcolumn of the new type that the marker type lacks must
-- read as the subcolumn of the converted column (printed next to it), not throw.

DROP TABLE IF EXISTS t_nullable;
DROP TABLE IF EXISTS t_nullable_compact;
DROP TABLE IF EXISTS t_tuple;
DROP TABLE IF EXISTS t_tuple_key;
DROP TABLE IF EXISTS t_nullable_tuple;
DROP TABLE IF EXISTS t_variant;
DROP TABLE IF EXISTS t_array;
DROP TABLE IF EXISTS t_map;

SELECT 'UInt64 -> Nullable(UInt64), Wide';
CREATE TABLE t_nullable (c0 UInt64, x UInt64) ENGINE = MergeTree ORDER BY c0
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
SYSTEM STOP MERGES t_nullable;
INSERT INTO t_nullable VALUES (1, 0), (2, 0);
INSERT INTO t_nullable VALUES (3, 5);
SELECT uniqExact(name), countIf(column = 'x') FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_nullable' AND active;
ALTER TABLE t_nullable MODIFY COLUMN x Nullable(UInt64) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_nullable' AND NOT is_done;
SELECT c0, x.null FROM t_nullable ORDER BY c0;
SELECT c0, x IS NULL FROM t_nullable ORDER BY c0 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_nullable WHERE x IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT c0 FROM t_nullable PREWHERE x IS NOT NULL ORDER BY c0 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT c0, x FROM t_nullable ORDER BY c0;

SELECT 'UInt64 -> Nullable(UInt64), Compact';
CREATE TABLE t_nullable_compact (c0 UInt64, x UInt64) ENGINE = MergeTree ORDER BY c0
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
SYSTEM STOP MERGES t_nullable_compact;
INSERT INTO t_nullable_compact VALUES (1, 0), (2, 0);
INSERT INTO t_nullable_compact VALUES (3, 5);
SELECT uniqExact(name), countIf(column = 'x') FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_nullable_compact' AND active;
ALTER TABLE t_nullable_compact MODIFY COLUMN x Nullable(UInt64) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_nullable_compact' AND NOT is_done;
SELECT c0, x.null FROM t_nullable_compact ORDER BY c0;
SELECT c0, x FROM t_nullable_compact ORDER BY c0;

SELECT 'Tuple(a) -> Tuple(a, b)';
CREATE TABLE t_tuple (c0 UInt64, x Tuple(a UInt64)) ENGINE = MergeTree ORDER BY c0
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
SYSTEM STOP MERGES t_tuple;
INSERT INTO t_tuple VALUES (1, tuple(0)), (2, tuple(0));
INSERT INTO t_tuple VALUES (3, tuple(5));
SELECT uniqExact(name), countIf(column = 'x') FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tuple' AND active;
ALTER TABLE t_tuple MODIFY COLUMN x Tuple(a UInt64, b UInt64) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_tuple' AND NOT is_done;
SELECT c0, x.b FROM t_tuple ORDER BY c0;
SELECT c0, x FROM t_tuple ORDER BY c0;

SELECT 'Tuple(a) -> Tuple(a, b), x.a in the sorting key';
CREATE TABLE t_tuple_key (c0 UInt64, x Tuple(a UInt64)) ENGINE = MergeTree ORDER BY (c0, x.a)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
SYSTEM STOP MERGES t_tuple_key;
INSERT INTO t_tuple_key VALUES (1, tuple(0)), (2, tuple(0));
INSERT INTO t_tuple_key VALUES (3, tuple(5));
SELECT uniqExact(name), countIf(column = 'x') FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_tuple_key' AND active;
ALTER TABLE t_tuple_key MODIFY COLUMN x Tuple(a UInt64, b UInt64) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_tuple_key' AND NOT is_done;
SELECT c0, x.b FROM t_tuple_key ORDER BY c0;
SELECT c0, x.a FROM t_tuple_key ORDER BY c0;
SELECT c0, x FROM t_tuple_key ORDER BY c0;

SELECT 'Nullable(Tuple(a)) -> Nullable(Tuple(a, b))';
CREATE TABLE t_nullable_tuple (c0 UInt64, x Nullable(Tuple(a UInt64))) ENGINE = MergeTree ORDER BY c0
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
SYSTEM STOP MERGES t_nullable_tuple;
INSERT INTO t_nullable_tuple VALUES (1, NULL), (2, NULL);
SELECT uniqExact(name), countIf(column = 'x') FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_nullable_tuple' AND active;
ALTER TABLE t_nullable_tuple MODIFY COLUMN x Nullable(Tuple(a UInt64, b UInt64)) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_nullable_tuple' AND NOT is_done;
SELECT c0, x.b FROM t_nullable_tuple ORDER BY c0;
SELECT c0, x.a FROM t_nullable_tuple ORDER BY c0;
SELECT c0, x FROM t_nullable_tuple ORDER BY c0;

SELECT 'Variant(UInt64) -> Variant(UInt64, String)';
CREATE TABLE t_variant (c0 UInt64, v Variant(UInt64)) ENGINE = MergeTree ORDER BY c0
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
SYSTEM STOP MERGES t_variant;
INSERT INTO t_variant VALUES (1, NULL), (2, NULL);
INSERT INTO t_variant VALUES (3, 5);
SELECT uniqExact(name), countIf(column = 'v') FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_variant' AND active;
ALTER TABLE t_variant MODIFY COLUMN v Variant(UInt64, String) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_variant' AND NOT is_done;
SELECT c0, v.String FROM t_variant ORDER BY c0;
SELECT c0, v.UInt64 FROM t_variant ORDER BY c0;
SELECT c0, v FROM t_variant ORDER BY c0;

SELECT 'Array(UInt64) -> Array(Nullable(UInt64))';
CREATE TABLE t_array (c0 UInt64, x Array(UInt64)) ENGINE = MergeTree ORDER BY c0
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
SYSTEM STOP MERGES t_array;
INSERT INTO t_array VALUES (1, []), (2, []);
INSERT INTO t_array VALUES (3, [5, 6]);
SELECT uniqExact(name), countIf(column = 'x') FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_array' AND active;
ALTER TABLE t_array MODIFY COLUMN x Array(Nullable(UInt64)) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_array' AND NOT is_done;
SELECT c0, x.null FROM t_array ORDER BY c0;
SELECT c0, x FROM t_array ORDER BY c0;

SELECT 'Map(String, UInt64) -> Map(String, Nullable(UInt64))';
CREATE TABLE t_map (c0 UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY c0
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
SYSTEM STOP MERGES t_map;
INSERT INTO t_map VALUES (1, {}), (2, {});
INSERT INTO t_map VALUES (3, {'k': 5});
SELECT uniqExact(name), countIf(column = 'm') FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_map' AND active;
ALTER TABLE t_map MODIFY COLUMN m Map(String, Nullable(UInt64)) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_map' AND NOT is_done;
SELECT c0, m.values.null FROM t_map ORDER BY c0;
SELECT c0, m FROM t_map ORDER BY c0;

DROP TABLE t_nullable;
DROP TABLE t_nullable_compact;
DROP TABLE t_tuple;
DROP TABLE t_tuple_key;
DROP TABLE t_nullable_tuple;
DROP TABLE t_variant;
DROP TABLE t_array;
DROP TABLE t_map;
