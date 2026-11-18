-- Tags: no-fasttest, zookeeper, no-shared-merge-tree
-- Tag no-fasttest: the executable function `test_function` is not configured in the fast test.
-- Tag zookeeper: the test uses a `ReplicatedMergeTree` table.
-- Tag no-shared-merge-tree: the affected rows are counted only for `MergeTree` and `ReplicatedMergeTree`.

-- Mutations that use an executable UDF (`test_function(a, b)`, which is defined in `tests/config/test_function.xml`,
-- returns `a + b`) must complete, also when they affect all rows of a part.
-- Executable UDFs are not registered in `FunctionFactory`, so the check for nondeterministic conditions
-- must resolve them through their own factory and read their `deterministic` flag, which is `false` for `test_function`.
-- test_function(a, b)  returns a + b

SET mutations_sync = 2;

SELECT '-- update all rows';
DROP TABLE IF EXISTS t_udf;
CREATE TABLE t_udf (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_udf SELECT number, number FROM numbers(10);
ALTER TABLE t_udf UPDATE v = test_function(v, 1) WHERE 1;
SELECT sum(v) FROM t_udf;
SELECT command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_udf';
DROP TABLE t_udf;

SELECT '-- delete all rows';
CREATE TABLE t_udf (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_udf SELECT number FROM numbers(10);
ALTER TABLE t_udf DELETE WHERE test_function(id, 1) > 0;
SELECT count() FROM t_udf;
SELECT command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_udf';
DROP TABLE t_udf;

-- Several commands that delete all rows together: the part is replaced by an empty one, and the conditions are not literals,
-- so the rows are not counted (a known limitation), but the mutations complete.
SELECT '-- delete all rows together';
CREATE TABLE t_udf (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_udf SELECT number FROM numbers(10);
SYSTEM STOP MERGES t_udf;
ALTER TABLE t_udf DELETE WHERE id < 3 SETTINGS mutations_sync = 0;
ALTER TABLE t_udf DELETE WHERE test_function(id, 1) > 0 SETTINGS mutations_sync = 0;
SYSTEM START MERGES t_udf;
ALTER TABLE t_udf DELETE WHERE 0;
SELECT count() FROM t_udf;
SELECT command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_udf' ORDER BY toUInt64(extract(mutation_id, '\\d+'));
DROP TABLE t_udf;

SELECT '-- update some rows';
CREATE TABLE t_udf (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_udf SELECT number, number FROM numbers(10);
ALTER TABLE t_udf UPDATE v = test_function(v, 1) WHERE id < 3;
SELECT sum(v) FROM t_udf;
SELECT command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_udf';
DROP TABLE t_udf;

-- A replicated table rejects nondeterministic mutations unless `allow_nondeterministic_mutations` is set.
SELECT '-- replicated rejected';
DROP TABLE IF EXISTS t_udf_replicated;
CREATE TABLE t_udf_replicated (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_udf_replicated', 'r1') ORDER BY id;
INSERT INTO t_udf_replicated SELECT number, number FROM numbers(10);
ALTER TABLE t_udf_replicated UPDATE v = test_function(v, 1) WHERE 1 SETTINGS allow_nondeterministic_mutations = 0; -- { serverError BAD_ARGUMENTS }

SELECT '-- replicated allowed';
ALTER TABLE t_udf_replicated UPDATE v = test_function(v, 1) WHERE 1 SETTINGS allow_nondeterministic_mutations = 1;
SELECT sum(v) FROM t_udf_replicated;
SELECT command, affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_udf_replicated';
DROP TABLE t_udf_replicated;
