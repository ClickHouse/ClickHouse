-- Under `analyzer_alias_hides_table_name`, an expression alias of an enclosing query hides the name of an aliased
-- table expression only when it can provide the rest of the identifier. The type of an alias that is not a constant
-- is learned by resolving it, so the answer does not depend on the order of the expressions of the enclosing query.

SET enable_analyzer = 1;
SET analyzer_alias_hides_table_name = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (val UInt64) ENGINE = Memory;
INSERT INTO t VALUES (10), (20), (30);

SELECT 'tuple without the element';
SELECT CAST(tuple(number), 'Tuple(other UInt64)') AS t, (SELECT count() FROM t AS c WHERE t.val > 15) AS n FROM numbers(1);
SELECT (SELECT count() FROM t AS c WHERE t.val > 15) AS n, CAST(tuple(number), 'Tuple(other UInt64)') AS t FROM numbers(1);
WITH CAST(tuple(materialize(1)), 'Tuple(other UInt64)') AS t SELECT (SELECT count() FROM t AS c WHERE t.val > 15) AS n;

SELECT 'scalar';
SELECT number + 1 AS t, (SELECT count() FROM t AS c WHERE t.val > 15) AS n FROM numbers(1);
SELECT (SELECT count() FROM t AS c WHERE t.val > 15) AS n, number + 1 AS t FROM numbers(1);
WITH materialize(1) AS t SELECT (SELECT count() FROM t AS c WHERE t.val > 15) AS n;

SELECT 'scalar subquery';
WITH (SELECT 1) AS t SELECT (SELECT count() FROM t AS c WHERE t.val > 15) AS n;
WITH (SELECT CAST(tuple(25), 'Tuple(val UInt64)')) AS t SELECT (SELECT count() FROM t AS c WHERE c.val < t.val) AS n;

SELECT 'alias of another alias';
WITH materialize(1) AS x, x AS t SELECT (SELECT count() FROM t AS c WHERE t.val > 15) AS n;
WITH CAST(tuple(25), 'Tuple(val UInt64)') AS x, x AS t SELECT (SELECT count() FROM t AS c WHERE c.val < t.val) AS n;

SELECT 'expression that only the subquery can resolve';
WITH val * 2 AS t SELECT (SELECT count() FROM t AS c WHERE t.val > 15) AS n;

SELECT 'tuple with the element';
SELECT CAST(tuple(number + 25), 'Tuple(val UInt64)') AS t, (SELECT count() FROM t AS c WHERE c.val < t.val) AS n FROM numbers(1);
SELECT (SELECT count() FROM t AS c WHERE c.val < t.val) AS n, CAST(tuple(number + 25), 'Tuple(val UInt64)') AS t FROM numbers(1);

SELECT 'a table of the enclosing query behind an alias without the element';
SELECT number + 1 AS t, (SELECT count() FROM t AS c WHERE c.val > t.val) AS n FROM (SELECT 0 AS number, 15 AS val) AS t;

SELECT 'matcher';
SELECT number + 1 AS t, (SELECT count() FROM (SELECT t.* FROM t AS c)) AS n FROM numbers(1);

SELECT 'unused aliases with the same name';
SELECT tuple(number AS t), tuple(number + 1 AS t), (SELECT count() FROM t AS c WHERE t.val > 15) AS n FROM numbers(1);

DROP TABLE t;
