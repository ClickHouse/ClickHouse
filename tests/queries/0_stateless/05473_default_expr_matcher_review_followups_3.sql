-- Tags: no-replicated-database
-- Tag no-replicated-database: the user-defined function is created on a single replica.

-- Renaming a column to the name of a lambda parameter must not let the parameter capture it.
DROP TABLE IF EXISTS t_rename_capture;
CREATE TABLE t_rename_capture
(
    a UInt8,
    b UInt8,
    c UInt8 DEFAULT identity(COLUMNS(a) APPLY (x -> x + b)),
    d UInt8 DEFAULT arrayMap(x -> x + b, [a])[1]
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_rename_capture (a, b) VALUES (7, 100);
ALTER TABLE t_rename_capture RENAME COLUMN b TO x;
INSERT INTO t_rename_capture (a, x) VALUES (7, 100);
SELECT c, d FROM t_rename_capture ORDER BY c;
DROP TABLE t_rename_capture;

-- A user-defined function applied by `APPLY` is inlined, so its column matchers are expanded against the schema.
DROP FUNCTION IF EXISTS f_05473_greatest_except_c;
CREATE FUNCTION f_05473_greatest_except_c AS v -> greatest(v, * EXCEPT c);

DROP TABLE IF EXISTS t_apply_udf;
CREATE TABLE t_apply_udf
(
    a UInt8,
    y UInt8 ALIAS a + 100,
    c UInt8 MATERIALIZED greatest(COLUMNS(a) APPLY f_05473_greatest_except_c),
    d UInt8 ALIAS greatest(COLUMNS(a) APPLY (z -> f_05473_greatest_except_c(z)))
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_apply_udf (a) VALUES (1);
SELECT c, d FROM t_apply_udf SETTINGS asterisk_include_alias_columns = 0;
SELECT c, d FROM t_apply_udf SETTINGS asterisk_include_alias_columns = 1;
ALTER TABLE t_apply_udf ADD COLUMN e UInt8 ALIAS identity(COLUMNS(a) APPLY f_05473_greatest_except_c);
SELECT e FROM t_apply_udf SETTINGS asterisk_include_alias_columns = 1;
DROP TABLE t_apply_udf;
DROP FUNCTION f_05473_greatest_except_c;

-- The old files of a skip index are not used while its rebuild (`MATERIALIZE INDEX` or `CLEAR INDEX`) is
-- pending, also when mutations are not applied on the fly.
DROP TABLE IF EXISTS t_pending_index;
CREATE TABLE t_pending_index
(
    src1 UInt64,
    al UInt64 ALIAS greatest(COLUMNS('^src')),
    INDEX idx al TYPE minmax GRANULARITY 1
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_pending_index (src1) VALUES (1);
SYSTEM STOP MERGES t_pending_index;
ALTER TABLE t_pending_index MODIFY COLUMN al ALIAS greatest(COLUMNS('^src')) + 999 SETTINGS alter_sync = 0, mutations_sync = 0;
SELECT count() FROM t_pending_index WHERE al = 1000;
SELECT count() FROM t_pending_index WHERE al = 1000 SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0;
DROP TABLE t_pending_index;

DROP TABLE IF EXISTS t_pending_index_drop;
CREATE TABLE t_pending_index_drop
(
    src1 UInt64,
    al UInt64 ALIAS greatest(COLUMNS('^src')),
    INDEX idx al TYPE minmax GRANULARITY 1
) ENGINE = MergeTree ORDER BY tuple() SETTINGS alter_column_secondary_index_mode = 'drop';
INSERT INTO t_pending_index_drop (src1) VALUES (1);
SYSTEM STOP MERGES t_pending_index_drop;
ALTER TABLE t_pending_index_drop MODIFY COLUMN al ALIAS greatest(COLUMNS('^src')) + 999 SETTINGS alter_sync = 0, mutations_sync = 0;
SELECT count() FROM t_pending_index_drop WHERE al = 1000;
SELECT count() FROM t_pending_index_drop WHERE al = 1000 SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0;
DROP TABLE t_pending_index_drop;
