-- Inserts reject invalid values of the `sign` column of `CollapsingMergeTree` and `VersionedCollapsingMergeTree` and of
-- the `is_deleted` column of `ReplacingMergeTree`, with or without the insert-time merge (`optimize_on_insert`).

DROP TABLE IF EXISTS t_replacing;
DROP TABLE IF EXISTS t_replacing_subcolumn;
DROP TABLE IF EXISTS t_collapsing;
DROP TABLE IF EXISTS t_versioned_collapsing;

SELECT '-- ReplacingMergeTree';
CREATE TABLE t_replacing (key UInt32, ver UInt32, is_deleted UInt8)
ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY key;

INSERT INTO t_replacing SETTINGS optimize_on_insert = 0 VALUES (1, 1, 0), (2, 1, 1);
INSERT INTO t_replacing SETTINGS optimize_on_insert = 0 VALUES (3, 1, 2); -- { serverError INCORRECT_DATA }
INSERT INTO t_replacing SETTINGS optimize_on_insert = 1 VALUES (3, 1, 2); -- { serverError INCORRECT_DATA }
-- A constant column.
INSERT INTO t_replacing SETTINGS optimize_on_insert = 0 SELECT number + 10, 1, 2 FROM numbers(3); -- { serverError INCORRECT_DATA }

-- With the check disabled the invalid value is accepted. A part with it cannot be merged, so stop merges.
SYSTEM STOP MERGES t_replacing;
ALTER TABLE t_replacing MODIFY SETTING add_implicit_is_deleted_column_constraint_for_replacing_engine = 0;
INSERT INTO t_replacing SETTINGS optimize_on_insert = 0 VALUES (3, 1, 2);
SELECT * FROM t_replacing ORDER BY key;

SELECT '-- ReplacingMergeTree with a subcolumn as is_deleted';
CREATE TABLE t_replacing_subcolumn (key UInt32, meta Tuple(ver UInt32, del UInt8))
ENGINE = ReplacingMergeTree(meta.ver, meta.del) ORDER BY key;

INSERT INTO t_replacing_subcolumn SETTINGS optimize_on_insert = 0 VALUES (1, (1, 0)), (2, (1, 1));
INSERT INTO t_replacing_subcolumn SETTINGS optimize_on_insert = 0 VALUES (3, (1, 2)); -- { serverError INCORRECT_DATA }
SELECT * FROM t_replacing_subcolumn ORDER BY key;

SELECT '-- CollapsingMergeTree';
CREATE TABLE t_collapsing (key UInt32, sign Int8)
ENGINE = CollapsingMergeTree(sign) ORDER BY key;

INSERT INTO t_collapsing SETTINGS optimize_on_insert = 0 VALUES (1, 1), (2, -1);
INSERT INTO t_collapsing SETTINGS optimize_on_insert = 0 VALUES (3, 5); -- { serverError INCORRECT_DATA }
INSERT INTO t_collapsing SETTINGS optimize_on_insert = 1 VALUES (3, 5); -- { serverError INCORRECT_DATA }
INSERT INTO t_collapsing SETTINGS optimize_on_insert = 0 SELECT number + 10, 0 FROM numbers(3); -- { serverError INCORRECT_DATA }
INSERT INTO t_collapsing SETTINGS optimize_on_insert = 0 SELECT number + 10, 1 FROM numbers(3);
SELECT * FROM t_collapsing ORDER BY key;

SELECT '-- VersionedCollapsingMergeTree';
CREATE TABLE t_versioned_collapsing (key UInt32, sign Int8, ver UInt32)
ENGINE = VersionedCollapsingMergeTree(sign, ver) ORDER BY key;

INSERT INTO t_versioned_collapsing SETTINGS optimize_on_insert = 0 VALUES (1, 1, 1), (2, -1, 1);
INSERT INTO t_versioned_collapsing SETTINGS optimize_on_insert = 0 VALUES (3, 0, 1); -- { serverError INCORRECT_DATA }
-- The insert-time merge of `VersionedCollapsingMergeTree` does not validate the sign, the check does.
INSERT INTO t_versioned_collapsing SETTINGS optimize_on_insert = 1 VALUES (3, 0, 1); -- { serverError INCORRECT_DATA }
SELECT * FROM t_versioned_collapsing ORDER BY key;

DROP TABLE t_replacing;
DROP TABLE t_replacing_subcolumn;
DROP TABLE t_collapsing;
DROP TABLE t_versioned_collapsing;
