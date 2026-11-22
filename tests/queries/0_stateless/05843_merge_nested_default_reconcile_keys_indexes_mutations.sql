-- Reconciling the `DEFAULT` of a missing `Nested` subcolumn with the shared offsets of a part changes
-- the value the expression produces. These cases cover what must and must not follow that change.

SET optimize_functions_to_subcolumns = 0;

SELECT 'A stored partition key column is not recomputed';
DROP TABLE IF EXISTS t_nested_reconcile_partition_key;
CREATE TABLE t_nested_reconcile_partition_key (id UInt32, k UInt64 MATERIALIZED id, `n.a` Array(UInt32))
ENGINE = MergeTree PARTITION BY k ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO t_nested_reconcile_partition_key (id, `n.a`) VALUES (1, [10, 20]), (2, [30]);
ALTER TABLE t_nested_reconcile_partition_key ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_reconcile_partition_key MODIFY COLUMN k UInt64 MATERIALIZED length(`n.b`);
OPTIMIZE TABLE t_nested_reconcile_partition_key FINAL;
SELECT id, k, `n.a`, `n.b` FROM t_nested_reconcile_partition_key ORDER BY id;
SELECT id, k FROM t_nested_reconcile_partition_key WHERE k = 2 ORDER BY id;
DROP TABLE t_nested_reconcile_partition_key;

SELECT 'A stored sorting key column is not recomputed';
DROP TABLE IF EXISTS t_nested_reconcile_sorting_key;
CREATE TABLE t_nested_reconcile_sorting_key (id UInt32, k UInt64 MATERIALIZED 3 - id, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY (k, id)
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO t_nested_reconcile_sorting_key (id, `n.a`) VALUES (1, [10, 20]), (2, [30]);
ALTER TABLE t_nested_reconcile_sorting_key ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_reconcile_sorting_key MODIFY COLUMN k UInt64 MATERIALIZED length(`n.b`);
OPTIMIZE TABLE t_nested_reconcile_sorting_key FINAL;
SELECT id, k, `n.a`, `n.b` FROM t_nested_reconcile_sorting_key ORDER BY k, id;
SELECT id, k FROM t_nested_reconcile_sorting_key WHERE k = 2 ORDER BY id;
DROP TABLE t_nested_reconcile_sorting_key;

SELECT 'A stored column with a non-deterministic function behind an alias is not recomputed';
DROP TABLE IF EXISTS t_nested_reconcile_alias;
DROP TABLE IF EXISTS t_nested_reconcile_alias_old;
CREATE TABLE t_nested_reconcile_alias (id UInt32, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO t_nested_reconcile_alias VALUES (1, [10, 20]), (2, [30]);
ALTER TABLE t_nested_reconcile_alias ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_reconcile_alias ADD COLUMN r UInt64 ALIAS rand64();
ALTER TABLE t_nested_reconcile_alias ADD COLUMN s UInt64 MATERIALIZED length(`n.b`) + r;
ALTER TABLE t_nested_reconcile_alias MATERIALIZE COLUMN s SETTINGS mutations_sync = 2;
CREATE TABLE t_nested_reconcile_alias_old ENGINE = Memory AS SELECT id, s FROM t_nested_reconcile_alias;
OPTIMIZE TABLE t_nested_reconcile_alias FINAL;
SELECT t.id, t.s != old.s, t.`n.a`, t.`n.b` FROM t_nested_reconcile_alias AS t JOIN t_nested_reconcile_alias_old AS old USING id ORDER BY id;
DROP TABLE t_nested_reconcile_alias;
DROP TABLE t_nested_reconcile_alias_old;

SELECT 'A text index over a recomputed stored column is rebuilt by the merge';
DROP TABLE IF EXISTS t_nested_reconcile_text_index;
CREATE TABLE t_nested_reconcile_text_index (id UInt32, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO t_nested_reconcile_text_index VALUES (1, [10, 20]), (2, [30]);
ALTER TABLE t_nested_reconcile_text_index ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_reconcile_text_index ADD COLUMN s String MATERIALIZED arrayStringConcat(`n.b`, ' ');
ALTER TABLE t_nested_reconcile_text_index MATERIALIZE COLUMN s SETTINGS mutations_sync = 2;
ALTER TABLE t_nested_reconcile_text_index ADD INDEX idx s TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1;
ALTER TABLE t_nested_reconcile_text_index MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
OPTIMIZE TABLE t_nested_reconcile_text_index FINAL;
SELECT id, s, `n.a`, `n.b` FROM t_nested_reconcile_text_index ORDER BY id;
SELECT count() FROM t_nested_reconcile_text_index WHERE hasToken(s, 'x') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_nested_reconcile_text_index WHERE hasToken(s, 'x') SETTINGS use_skip_indexes = 1;
DROP TABLE t_nested_reconcile_text_index;

SELECT 'A projection is rebuilt by an unrelated mutation that rewrites the whole part';
DROP TABLE IF EXISTS t_nested_reconcile_mutation_projection;
CREATE TABLE t_nested_reconcile_mutation_projection (id UInt32, obj JSON, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO t_nested_reconcile_mutation_projection VALUES (1, '{"x":1}', [10, 20]), (2, '{"x":2}', [30, 40]);
ALTER TABLE t_nested_reconcile_mutation_projection ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_reconcile_mutation_projection ADD PROJECTION p (SELECT id, `n.b` ORDER BY `n.b`);
ALTER TABLE t_nested_reconcile_mutation_projection MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
ALTER TABLE t_nested_reconcile_mutation_projection UPDATE obj = '{"x":3}' WHERE 1 SETTINGS mutations_sync = 2;
SELECT id, `n.a`, `n.b` FROM t_nested_reconcile_mutation_projection ORDER BY id;
SELECT count() FROM t_nested_reconcile_mutation_projection WHERE `n.b` = ['', ''] SETTINGS optimize_use_projections = 0;
SELECT count() FROM t_nested_reconcile_mutation_projection WHERE `n.b` = ['', ''] SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
DROP TABLE t_nested_reconcile_mutation_projection;

SELECT 'MATERIALIZE COLUMN of the missing subcolumn keeps the shared offsets';
DROP TABLE IF EXISTS t_nested_reconcile_materialize_column;
CREATE TABLE t_nested_reconcile_materialize_column (id UInt32, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO t_nested_reconcile_materialize_column VALUES (1, [10, 20]), (2, [30]);
ALTER TABLE t_nested_reconcile_materialize_column ADD COLUMN `n.b` Array(String) DEFAULT ['x'];
ALTER TABLE t_nested_reconcile_materialize_column MATERIALIZE COLUMN `n.b` SETTINGS mutations_sync = 2;
SELECT id, `n.a`, `n.b` FROM t_nested_reconcile_materialize_column ORDER BY id;
CHECK TABLE t_nested_reconcile_materialize_column SETTINGS check_query_single_value_result = 1;
DROP TABLE t_nested_reconcile_materialize_column;
