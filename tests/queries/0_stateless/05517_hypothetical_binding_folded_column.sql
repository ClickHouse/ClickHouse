-- the binding keeps a column that constant folding removes from the analysis, such as `b` in `isNull(b)`

SET enable_parallel_replicas = 0;

SELECT 'a type change of a folded column makes the index not applicable, also through an ALIAS column';
DROP TABLE IF EXISTS t_fold;
CREATE TABLE t_fold (a UInt64, b UInt64, c UInt64 ALIAS a + isNull(b)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100;
INSERT INTO t_fold SELECT number, number FROM numbers(1000);
CREATE HYPOTHETICAL INDEX i ON t_fold (a + isNull(b)) TYPE minmax;
CREATE HYPOTHETICAL INDEX j ON t_fold (c) TYPE minmax;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a FROM t_fold WHERE a = 42) WHERE explain LIKE 'With %' OR explain LIKE '  status:%' OR explain LIKE '  reason:%';
ALTER TABLE t_fold MODIFY COLUMN b Nullable(UInt64) SETTINGS mutations_sync = 2;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a FROM t_fold WHERE a = 42) WHERE explain LIKE 'With %' OR explain LIKE '  status:%' OR explain LIKE '  reason:%';
DROP TABLE t_fold;

SELECT 'a drop of a folded column makes the index not applicable';
DROP TABLE IF EXISTS t_fold_drop;
CREATE TABLE t_fold_drop (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100;
INSERT INTO t_fold_drop SELECT number, number FROM numbers(1000);
CREATE HYPOTHETICAL INDEX i ON t_fold_drop (a + isNull(b)) TYPE minmax;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a FROM t_fold_drop WHERE a = 42) WHERE explain LIKE '  status:%' OR explain LIKE '  reason:%';
ALTER TABLE t_fold_drop DROP COLUMN b SETTINGS mutations_sync = 2;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a FROM t_fold_drop WHERE a = 42) WHERE explain LIKE '  status:%' OR explain LIKE '  reason:%';
DROP TABLE t_fold_drop;
