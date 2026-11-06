-- the binding follows the columns the definition reads: lambda parameters mask columns, and matchers expand over all columns

SET enable_parallel_replicas = 0, optimize_use_projections = 1, enable_positional_arguments_for_projections = 1;

SELECT 'a MATERIALIZED column under * makes the projection not applicable, as ADD PROJECTION rejects it';
DROP TABLE IF EXISTS t_star;
CREATE TABLE t_star (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100;
INSERT INTO t_star SELECT number % 10, number % 7 FROM numbers(1000);
CREATE HYPOTHETICAL PROJECTION p ON t_star (SELECT *, count() GROUP BY 1, 2);
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a, b, count() FROM t_star GROUP BY a, b) WHERE explain LIKE '  status:%';
ALTER TABLE t_star ADD COLUMN c UInt64 MATERIALIZED a + 1 FIRST;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a, b, count() FROM t_star GROUP BY a, b) WHERE explain LIKE '  status:%' OR explain LIKE '  reason:%';
ALTER TABLE t_star ADD PROJECTION p (SELECT *, count() GROUP BY 1, 2); -- { serverError NOT_AN_AGGREGATE }
DROP TABLE t_star;

SELECT 'a column with the name of a lambda parameter does not bind the index';
DROP TABLE IF EXISTS t_lambda;
CREATE TABLE t_lambda (a Array(UInt64), x UInt64, y UInt64 ALIAS x + 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100;
INSERT INTO t_lambda SELECT [number, number + 1], number FROM numbers(1000);
CREATE HYPOTHETICAL INDEX i ON t_lambda (arraySum(arrayMap((x, y) -> x + y, a, a))) TYPE minmax;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT count() FROM t_lambda WHERE arraySum(arrayMap((x, y) -> x + y, a, a)) = 6) WHERE explain LIKE '  status:%';
ALTER TABLE t_lambda MODIFY COLUMN x UInt32 SETTINGS mutations_sync = 2;
ALTER TABLE t_lambda MODIFY COLUMN y UInt64 ALIAS x + 2;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT count() FROM t_lambda WHERE arraySum(arrayMap((x, y) -> x + y, a, a)) = 6) WHERE explain LIKE '  status:%' OR explain LIKE '  reason:%';
DROP TABLE t_lambda;

SELECT 'COLUMNS binds only the columns that match';
DROP TABLE IF EXISTS t_regexp;
CREATE TABLE t_regexp (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100;
INSERT INTO t_regexp SELECT number % 10, number FROM numbers(1000);
CREATE HYPOTHETICAL PROJECTION p ON t_regexp (SELECT COLUMNS('^a'), count() GROUP BY a);
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a, count() FROM t_regexp GROUP BY a) WHERE explain LIKE '  status:%';
ALTER TABLE t_regexp ADD COLUMN z UInt64;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a, count() FROM t_regexp GROUP BY a) WHERE explain LIKE '  status:%' OR explain LIKE '  reason:%';
ALTER TABLE t_regexp ADD COLUMN a2 UInt64;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a, count() FROM t_regexp GROUP BY a) WHERE explain LIKE '  status:%' OR explain LIKE '  reason:%';
ALTER TABLE t_regexp ADD PROJECTION p (SELECT COLUMNS('^a'), count() GROUP BY a); -- { serverError NOT_AN_AGGREGATE }
DROP TABLE t_regexp;
