-- A column added under the name of a subcolumn must not change what an aggregate projection or a sorting key reads.

-- An aggregate projection holds states computed from the sizes of `a`.
DROP TABLE IF EXISTS t_aggregate_projection;
CREATE TABLE t_aggregate_projection (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_aggregate_projection VALUES ([0]), ([1, 2]);
ALTER TABLE t_aggregate_projection ADD PROJECTION p (SELECT sum(`a.size0`));
ALTER TABLE t_aggregate_projection MATERIALIZE PROJECTION p SETTINGS mutations_sync = 1;
ALTER TABLE t_aggregate_projection ADD COLUMN `a.size0` UInt64 DEFAULT 7; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT sum(`a.size0`) FROM t_aggregate_projection SETTINGS optimize_use_projections = 0;
SELECT sum(`a.size0`) FROM t_aggregate_projection SETTINGS optimize_use_projections = 1;
DROP TABLE t_aggregate_projection;

-- A column added to the sorting key in the same `ALTER`.
DROP TABLE IF EXISTS t_modify_order_by;
CREATE TABLE t_modify_order_by (id UInt64, a Array(UInt64)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_modify_order_by ADD COLUMN `a.size0` UInt64, MODIFY ORDER BY (id, `a.size0`); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_modify_order_by ADD COLUMN `b` UInt64, MODIFY ORDER BY (id, b);
DROP TABLE t_modify_order_by;

-- A part that records the key column as missing reads its default, and pruning agrees with it.
DROP TABLE IF EXISTS t_missing_marker;
CREATE TABLE t_missing_marker (a Array(UInt64), `a.size0` UInt64) ENGINE = MergeTree ORDER BY `a.size0`
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns', index_granularity = 1;
INSERT INTO t_missing_marker (a) VALUES ([1, 2, 3]);
SELECT a, `a.size0` FROM t_missing_marker;
SELECT count() FROM t_missing_marker WHERE `a.size0` = 0;
SELECT countIf(`a.size0` = 0) FROM t_missing_marker;
DROP TABLE t_missing_marker;

-- The same for a flattened `Array` column of a wide part, read as a subcolumn of a `Nested` column next to a `JSON`
-- column of the same name: the marker is stored under the name of the flattened column.
DROP TABLE IF EXISTS t_missing_marker_nested;
CREATE TABLE t_missing_marker_nested (id UInt64, a JSON, `a.x` Array(UInt64)) ENGINE = MergeTree ORDER BY length(`a.x`)
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns', index_granularity = 1,
    share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_level_for_wide_part = 0;
INSERT INTO t_missing_marker_nested (id, a) VALUES (1, '{"x":[11,22]}');
SELECT id, `a.x`, length(`a.x`) FROM t_missing_marker_nested;
SELECT count() FROM t_missing_marker_nested WHERE length(`a.x`) = 0;
SELECT countIf(length(`a.x`) = 0) FROM t_missing_marker_nested;
DROP TABLE t_missing_marker_nested;
