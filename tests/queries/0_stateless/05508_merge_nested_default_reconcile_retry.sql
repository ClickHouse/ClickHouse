-- Reconciliation of `Nested` defaults with the shared offsets during merges: dependents reached
-- through a subcolumn of an intermediate, non-deterministic prerequisites, and mutations that leave
-- the reconciled column missing. The mutations hardlink the untouched columns of a part with full
-- storage only; a part with packed storage is rewritten as a whole (see
-- `05628_merge_nested_default_reconcile_stored_dependents`).

SELECT '-- dependency through a subcolumn of an intermediate';
DROP TABLE IF EXISTS t_nested_reconcile_subcolumn;
CREATE TABLE t_nested_reconcile_subcolumn (id UInt32, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, enable_vertical_merge_algorithm = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO t_nested_reconcile_subcolumn VALUES (1, [10, 20]);
ALTER TABLE t_nested_reconcile_subcolumn ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_reconcile_subcolumn ADD COLUMN tmp Array(String) DEFAULT `n.b`;
ALTER TABLE t_nested_reconcile_subcolumn ADD COLUMN `n.c` Array(String) DEFAULT arrayWithConstant(tmp.size0, 'z');
OPTIMIZE TABLE t_nested_reconcile_subcolumn FINAL;
SELECT `n.b`, tmp, `n.c` FROM t_nested_reconcile_subcolumn;
DROP TABLE t_nested_reconcile_subcolumn;

SELECT '-- non-deterministic prerequisite';
DROP TABLE IF EXISTS t_nested_reconcile_rand;
CREATE TABLE t_nested_reconcile_rand (id UInt32, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, enable_vertical_merge_algorithm = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO t_nested_reconcile_rand VALUES (1, [10]), (2, [20, 30]);
ALTER TABLE t_nested_reconcile_rand ADD COLUMN r UInt64 DEFAULT rand64();
ALTER TABLE t_nested_reconcile_rand ADD COLUMN `n.b` Array(UInt64) MATERIALIZED [r];
INSERT INTO t_nested_reconcile_rand (id, `n.a`) VALUES (3, [40]);
OPTIMIZE TABLE t_nested_reconcile_rand FINAL;
SELECT id, `n.b` = [r], length(`n.b`) FROM t_nested_reconcile_rand ORDER BY id;
DROP TABLE t_nested_reconcile_rand;

SELECT '-- projection materialized by a mutation';
DROP TABLE IF EXISTS t_nested_reconcile_projection;
CREATE TABLE t_nested_reconcile_projection (id UInt32, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, max_bytes_to_merge_at_max_space_in_pool = 1, min_bytes_for_full_part_storage = 0;
INSERT INTO t_nested_reconcile_projection VALUES (1, [10, 20]);
ALTER TABLE t_nested_reconcile_projection ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_reconcile_projection ADD PROJECTION p (SELECT id, `n.b` ORDER BY id);
ALTER TABLE t_nested_reconcile_projection MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
SELECT `n.b` FROM t_nested_reconcile_projection SETTINGS optimize_use_projections = 0;
SELECT `n.b` FROM t_nested_reconcile_projection SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
DROP TABLE t_nested_reconcile_projection;

SELECT '-- dependent column materialized by a mutation';
DROP TABLE IF EXISTS t_nested_reconcile_materialize;
CREATE TABLE t_nested_reconcile_materialize (id UInt32, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, max_bytes_to_merge_at_max_space_in_pool = 1, min_bytes_for_full_part_storage = 0;
INSERT INTO t_nested_reconcile_materialize VALUES (1, [10, 20]);
ALTER TABLE t_nested_reconcile_materialize ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_reconcile_materialize ADD COLUMN b_length UInt64 MATERIALIZED length(`n.b`);
ALTER TABLE t_nested_reconcile_materialize MATERIALIZE COLUMN b_length SETTINGS mutations_sync = 2;
-- `length` is computed from the full column: the subcolumn `n.b.size0` of a missing column is read
-- from the shared offsets, which is a separate matter.
SELECT `n.b`, b_length, length(`n.b`) FROM t_nested_reconcile_materialize SETTINGS optimize_functions_to_subcolumns = 0;
DROP TABLE t_nested_reconcile_materialize;
