-- Under `analyzer_alias_hides_table_name`, the type of an expression alias of an enclosing query decides whether the
-- alias hides the name of an aliased table expression. Asking for the type must leave no trace in the query: when the
-- alias does not provide the identifier, the query does not use it, so it is not evaluated, its columns do not make
-- the subquery correlated, the aliases inside it are not counted as used, and an error in it is not reported.

SET enable_analyzer = 1;
SET analyzer_alias_hides_table_name = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (val UInt64) ENGINE = Memory;
INSERT INTO t VALUES (10), (20), (30);

SELECT 'a column of the enclosing query does not make the subquery correlated';
WITH number + 1 AS t SELECT number IN (SELECT t.val FROM t AS c) FROM numbers(1);

SELECT 'an expression that throws is not evaluated';
WITH intDiv(1, 0) AS t SELECT (SELECT count() FROM t AS c WHERE t.val > 15);

SELECT 'a scalar subquery is not executed';
WITH (SELECT throwIf(1)) AS t SELECT (SELECT count() FROM t AS c WHERE t.val > 15);

SELECT 'an alias inside the expression is not counted as used';
WITH x AS t SELECT (SELECT tuple(5 AS x), tuple(6 AS x), count() FROM t AS c WHERE t.val > 15);

SELECT 'an expression that cannot be resolved';
WITH unknown_identifier + 1 AS t SELECT (SELECT count() FROM t AS c WHERE t.val > 15);

SELECT 'an alias that provides the identifier is still used';
WITH CAST(tuple(number + 25), 'Tuple(val UInt64)') AS t SELECT (SELECT count() FROM t AS c WHERE c.val < t.val) FROM numbers(1);
WITH (SELECT CAST(tuple(25), 'Tuple(val UInt64)')) AS t SELECT (SELECT count() FROM t AS c WHERE c.val < t.val);

DROP TABLE t;
