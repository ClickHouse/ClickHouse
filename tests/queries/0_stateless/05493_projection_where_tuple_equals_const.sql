-- A projection `WHERE` comparing a `Tuple` column with a constant turns the column into a `ColumnConst`,
-- which must be materialized before the projection part is written: on insert, merge, and mutation.
-- https://github.com/ClickHouse/ClickHouse/issues/128533
-- `ratio_of_defaults_for_sparse_serialization` must stay below 1: at 1 no serialization infos are collected,
-- so the code path is not reached. It is pinned because the test runner randomizes it.

SET optimize_constant_columns_after_filter = 1;

DROP TABLE IF EXISTS t_proj_tuple_eq;

CREATE TABLE t_proj_tuple_eq
(
    k UInt64,
    tup Tuple(UInt64, UInt64),
    PROJECTION p (SELECT k, tup WHERE tup = (1, 2) ORDER BY k)
)
ENGINE = MergeTree ORDER BY k
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9375;

INSERT INTO t_proj_tuple_eq VALUES (1, (1, 2)), (2, (3, 4)), (3, (1, 2));

ALTER TABLE t_proj_tuple_eq CLEAR PROJECTION p SETTINGS mutations_sync = 1;
ALTER TABLE t_proj_tuple_eq MATERIALIZE PROJECTION p SETTINGS mutations_sync = 1;

SELECT sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_tuple_eq' AND name = 'p' AND active;

SELECT k, tup FROM t_proj_tuple_eq ORDER BY k;
SELECT k, tup FROM mergeTreeProjection(currentDatabase(), t_proj_tuple_eq, p) ORDER BY k;

DROP TABLE t_proj_tuple_eq;

-- The same projection calculated by a merge of parts which do not have it yet.
DROP TABLE IF EXISTS t_proj_tuple_eq_merge;

CREATE TABLE t_proj_tuple_eq_merge
(
    k UInt64,
    tup Tuple(UInt64, UInt64),
    PROJECTION p (SELECT k, tup WHERE tup = (1, 2) ORDER BY k)
)
ENGINE = MergeTree ORDER BY k
SETTINGS materialize_projections_on_insert = 0, materialize_projections_on_merge = 1, ratio_of_defaults_for_sparse_serialization = 0.9375;

INSERT INTO t_proj_tuple_eq_merge VALUES (1, (1, 2)), (2, (3, 4));
INSERT INTO t_proj_tuple_eq_merge VALUES (3, (1, 2)), (4, (3, 4));

OPTIMIZE TABLE t_proj_tuple_eq_merge FINAL;

SELECT sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_tuple_eq_merge' AND name = 'p' AND active;

SELECT k, tup FROM mergeTreeProjection(currentDatabase(), t_proj_tuple_eq_merge, p) ORDER BY k;

DROP TABLE t_proj_tuple_eq_merge;

-- The same projection added to a table with existing data and calculated by a mutation.
DROP TABLE IF EXISTS t_proj_tuple_eq_mutation;

CREATE TABLE t_proj_tuple_eq_mutation
(
    k UInt64,
    tup Tuple(UInt64, UInt64)
)
ENGINE = MergeTree ORDER BY k
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9375;

INSERT INTO t_proj_tuple_eq_mutation VALUES (1, (1, 2)), (2, (3, 4)), (3, (1, 2));

ALTER TABLE t_proj_tuple_eq_mutation ADD PROJECTION p (SELECT k, tup WHERE tup = (1, 2) ORDER BY k);
ALTER TABLE t_proj_tuple_eq_mutation MATERIALIZE PROJECTION p SETTINGS mutations_sync = 1;

SELECT sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_tuple_eq_mutation' AND name = 'p' AND active;

SELECT k, tup FROM mergeTreeProjection(currentDatabase(), t_proj_tuple_eq_mutation, p) ORDER BY k;

DROP TABLE t_proj_tuple_eq_mutation;
