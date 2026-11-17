-- A merge reconciles a `Nested` default with the shared offsets of the source parts. The data
-- derived from the default before the merge must agree with the reconciled value afterwards: a
-- projection materialized before the merge is rebuilt, and a stored deterministic `MATERIALIZED`
-- column computed from the default is computed again. A mutation that rewrites the whole part
-- (packed part storage) reconciles the defaults it writes as a merge does.

SET optimize_functions_to_subcolumns = 0;

SELECT '-- projection materialized before the merge: horizontal, full storage';
DROP TABLE IF EXISTS t_nested_reconcile_proj;
CREATE TABLE t_nested_reconcile_proj (id UInt32, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_nested_reconcile_proj VALUES (1, [10, 20]);
INSERT INTO t_nested_reconcile_proj VALUES (2, [30, 40]);
ALTER TABLE t_nested_reconcile_proj ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_reconcile_proj ADD PROJECTION p (SELECT id, `n.b` ORDER BY id);
ALTER TABLE t_nested_reconcile_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
OPTIMIZE TABLE t_nested_reconcile_proj FINAL;
SELECT id, `n.b` FROM t_nested_reconcile_proj ORDER BY id SETTINGS optimize_use_projections = 0;
SELECT * FROM (SELECT id, `n.b` FROM t_nested_reconcile_proj SETTINGS optimize_use_projections = 1, force_optimize_projection = 1) ORDER BY id;
DROP TABLE t_nested_reconcile_proj;

SELECT '-- dependent column materialized before the merge: horizontal, full storage';
DROP TABLE IF EXISTS t_nested_reconcile_dep;
CREATE TABLE t_nested_reconcile_dep (id UInt32, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_nested_reconcile_dep VALUES (1, [10, 20]);
INSERT INTO t_nested_reconcile_dep VALUES (2, [30, 40]);
ALTER TABLE t_nested_reconcile_dep ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_reconcile_dep ADD COLUMN b_length UInt64 MATERIALIZED length(`n.b`);
ALTER TABLE t_nested_reconcile_dep MATERIALIZE COLUMN b_length SETTINGS mutations_sync = 2;
OPTIMIZE TABLE t_nested_reconcile_dep FINAL;
SELECT id, `n.b`, b_length, length(`n.b`) FROM t_nested_reconcile_dep ORDER BY id;
DROP TABLE t_nested_reconcile_dep;

SELECT '-- projection materialized before the merge: vertical, packed storage';
DROP TABLE IF EXISTS t_nested_reconcile_proj;
CREATE TABLE t_nested_reconcile_proj (id UInt32, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 0, vertical_merge_algorithm_min_columns_to_activate = 0, min_bytes_for_full_part_storage = 1000000000;
INSERT INTO t_nested_reconcile_proj VALUES (1, [10, 20]);
INSERT INTO t_nested_reconcile_proj VALUES (2, [30, 40]);
ALTER TABLE t_nested_reconcile_proj ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_reconcile_proj ADD PROJECTION p (SELECT id, `n.b` ORDER BY id);
ALTER TABLE t_nested_reconcile_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
OPTIMIZE TABLE t_nested_reconcile_proj FINAL;
SELECT id, `n.b` FROM t_nested_reconcile_proj ORDER BY id SETTINGS optimize_use_projections = 0;
SELECT * FROM (SELECT id, `n.b` FROM t_nested_reconcile_proj SETTINGS optimize_use_projections = 1, force_optimize_projection = 1) ORDER BY id;
DROP TABLE t_nested_reconcile_proj;

SELECT '-- dependent column materialized before the merge: vertical, packed storage';
DROP TABLE IF EXISTS t_nested_reconcile_dep;
CREATE TABLE t_nested_reconcile_dep (id UInt32, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 0, vertical_merge_algorithm_min_columns_to_activate = 0, min_bytes_for_full_part_storage = 1000000000;
INSERT INTO t_nested_reconcile_dep VALUES (1, [10, 20]);
INSERT INTO t_nested_reconcile_dep VALUES (2, [30, 40]);
ALTER TABLE t_nested_reconcile_dep ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_reconcile_dep ADD COLUMN b_length UInt64 MATERIALIZED length(`n.b`);
ALTER TABLE t_nested_reconcile_dep MATERIALIZE COLUMN b_length SETTINGS mutations_sync = 2;
OPTIMIZE TABLE t_nested_reconcile_dep FINAL;
SELECT id, `n.b`, b_length, length(`n.b`) FROM t_nested_reconcile_dep ORDER BY id;
DROP TABLE t_nested_reconcile_dep;

SELECT '-- projection materialized before the merge: vertical, full storage';
DROP TABLE IF EXISTS t_nested_reconcile_proj;
CREATE TABLE t_nested_reconcile_proj (id UInt32, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 0, vertical_merge_algorithm_min_columns_to_activate = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_nested_reconcile_proj VALUES (1, [10, 20]);
INSERT INTO t_nested_reconcile_proj VALUES (2, [30, 40]);
ALTER TABLE t_nested_reconcile_proj ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_reconcile_proj ADD PROJECTION p (SELECT id, `n.b` ORDER BY id);
ALTER TABLE t_nested_reconcile_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
OPTIMIZE TABLE t_nested_reconcile_proj FINAL;
SELECT id, `n.b` FROM t_nested_reconcile_proj ORDER BY id SETTINGS optimize_use_projections = 0;
SELECT * FROM (SELECT id, `n.b` FROM t_nested_reconcile_proj SETTINGS optimize_use_projections = 1, force_optimize_projection = 1) ORDER BY id;
DROP TABLE t_nested_reconcile_proj;

SELECT '-- dependent column materialized before the merge: vertical, full storage';
DROP TABLE IF EXISTS t_nested_reconcile_dep;
CREATE TABLE t_nested_reconcile_dep (id UInt32, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 0, vertical_merge_algorithm_min_columns_to_activate = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_nested_reconcile_dep VALUES (1, [10, 20]);
INSERT INTO t_nested_reconcile_dep VALUES (2, [30, 40]);
ALTER TABLE t_nested_reconcile_dep ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_reconcile_dep ADD COLUMN b_length UInt64 MATERIALIZED length(`n.b`);
ALTER TABLE t_nested_reconcile_dep MATERIALIZE COLUMN b_length SETTINGS mutations_sync = 2;
OPTIMIZE TABLE t_nested_reconcile_dep FINAL;
SELECT id, `n.b`, b_length, length(`n.b`) FROM t_nested_reconcile_dep ORDER BY id;
DROP TABLE t_nested_reconcile_dep;

SELECT '-- projection materialized before the merge: horizontal, packed storage';
DROP TABLE IF EXISTS t_nested_reconcile_proj;
CREATE TABLE t_nested_reconcile_proj (id UInt32, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0, min_bytes_for_full_part_storage = 1000000000;
INSERT INTO t_nested_reconcile_proj VALUES (1, [10, 20]);
INSERT INTO t_nested_reconcile_proj VALUES (2, [30, 40]);
ALTER TABLE t_nested_reconcile_proj ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_reconcile_proj ADD PROJECTION p (SELECT id, `n.b` ORDER BY id);
ALTER TABLE t_nested_reconcile_proj MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
OPTIMIZE TABLE t_nested_reconcile_proj FINAL;
SELECT id, `n.b` FROM t_nested_reconcile_proj ORDER BY id SETTINGS optimize_use_projections = 0;
SELECT * FROM (SELECT id, `n.b` FROM t_nested_reconcile_proj SETTINGS optimize_use_projections = 1, force_optimize_projection = 1) ORDER BY id;
DROP TABLE t_nested_reconcile_proj;

SELECT '-- dependent column materialized before the merge: horizontal, packed storage';
DROP TABLE IF EXISTS t_nested_reconcile_dep;
CREATE TABLE t_nested_reconcile_dep (id UInt32, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0, min_bytes_for_full_part_storage = 1000000000;
INSERT INTO t_nested_reconcile_dep VALUES (1, [10, 20]);
INSERT INTO t_nested_reconcile_dep VALUES (2, [30, 40]);
ALTER TABLE t_nested_reconcile_dep ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_reconcile_dep ADD COLUMN b_length UInt64 MATERIALIZED length(`n.b`);
ALTER TABLE t_nested_reconcile_dep MATERIALIZE COLUMN b_length SETTINGS mutations_sync = 2;
OPTIMIZE TABLE t_nested_reconcile_dep FINAL;
SELECT id, `n.b`, b_length, length(`n.b`) FROM t_nested_reconcile_dep ORDER BY id;
DROP TABLE t_nested_reconcile_dep;

SELECT '-- projection materialized by a mutation of a part with packed storage';
DROP TABLE IF EXISTS t_nested_reconcile_packed;
CREATE TABLE t_nested_reconcile_packed (id UInt32, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, max_bytes_to_merge_at_max_space_in_pool = 1, min_bytes_for_full_part_storage = 1000000000;
INSERT INTO t_nested_reconcile_packed VALUES (1, [10, 20]);
ALTER TABLE t_nested_reconcile_packed ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_reconcile_packed ADD PROJECTION p (SELECT id, `n.b` ORDER BY id);
ALTER TABLE t_nested_reconcile_packed MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
SELECT `n.b` FROM t_nested_reconcile_packed SETTINGS optimize_use_projections = 0;
SELECT `n.b` FROM t_nested_reconcile_packed SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
CHECK TABLE t_nested_reconcile_packed SETTINGS check_query_single_value_result = 1;
DROP TABLE t_nested_reconcile_packed;
