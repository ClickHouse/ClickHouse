-- with no read step in the plan, EXPLAIN WHATIF finds the table only when the query reads one MergeTree table

DROP TABLE IF EXISTS t_empty;
DROP TABLE IF EXISTS t_other;
CREATE TABLE t_empty (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_other (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE HYPOTHETICAL INDEX i_a ON t_empty (a) TYPE minmax;

SELECT 'one table';
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a FROM t_empty WHERE a = 1) WHERE explain LIKE '  reason:%';

SELECT 'a subquery';
EXPLAIN WHATIF SELECT a FROM (SELECT a FROM t_empty) WHERE a = 1; -- { serverError NOT_IMPLEMENTED }

SELECT 'a union';
EXPLAIN WHATIF SELECT a FROM t_empty WHERE a = 1 UNION ALL SELECT a FROM t_other WHERE a = 1; -- { serverError NOT_IMPLEMENTED }

SELECT 'a join';
EXPLAIN WHATIF SELECT t_empty.a FROM t_empty JOIN t_other ON t_empty.a = t_other.a WHERE t_empty.a = 1; -- { serverError NOT_IMPLEMENTED }

SELECT 'a table function';
EXPLAIN WHATIF SELECT number FROM numbers(10) WHERE number = 1; -- { serverError NOT_IMPLEMENTED }

SELECT 'no FROM';
EXPLAIN WHATIF SELECT 1; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_empty;
DROP TABLE t_other;
