-- The two sides of a join of a table with itself are different column sources.
-- A column of one side must not be considered equal to a GROUP BY key that is a column of the other side.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_self_join;
CREATE TABLE t_self_join (x UInt64, y UInt64) ENGINE = Memory;
INSERT INTO t_self_join VALUES (1, 10), (2, 20);

SELECT 'group_by_use_nulls';

-- The column of the other side was replaced with the Nullable clone of the key.
SELECT r.number
FROM numbers(2) AS l INNER JOIN numbers(2) AS r ON l.number = r.number
GROUP BY l.number WITH ROLLUP
SETTINGS group_by_use_nulls = 1; -- { serverError NOT_AN_AGGREGATE }

SELECT l.number
FROM numbers(2) AS l INNER JOIN numbers(2) AS r ON l.number = r.number
GROUP BY l.number WITH ROLLUP
ORDER BY l.number
SETTINGS group_by_use_nulls = 1;

SELECT 'reused alias of a table expression';

SELECT r.number
FROM (SELECT number FROM numbers(2)) AS l JOIN l AS r ON l.number = r.number
GROUP BY l.number WITH ROLLUP
SETTINGS group_by_use_nulls = 1; -- { serverError NOT_AN_AGGREGATE }

SELECT l.number
FROM (SELECT number FROM numbers(2)) AS l JOIN l AS r ON l.number = r.number
GROUP BY l.number WITH ROLLUP
ORDER BY l.number
SETTINGS group_by_use_nulls = 1;

SELECT 'CTE';

WITH c AS (SELECT number FROM numbers(2))
SELECT r.number
FROM c AS l JOIN c AS r ON l.number = r.number
GROUP BY l.number; -- { serverError NOT_AN_AGGREGATE }

WITH c AS (SELECT number FROM numbers(2))
SELECT l.number, count()
FROM c AS l JOIN c AS r ON l.number = r.number
GROUP BY l.number
ORDER BY l.number;

SELECT 'table expressions without aliases';

-- `COLUMNS('^x$')` expands to `x` of the left table and `x` of the right table.
SELECT minus(COLUMNS('^x$')) AS d
FROM t_self_join JOIN {CLICKHOUSE_DATABASE:Identifier}.t_self_join ON 1
GROUP BY t_self_join.x; -- { serverError NOT_AN_AGGREGATE }

SELECT t_self_join.x
FROM t_self_join JOIN {CLICKHOUSE_DATABASE:Identifier}.t_self_join ON 1
GROUP BY t_self_join.x
ORDER BY t_self_join.x;

SELECT 'grouping';

SELECT grouping(r.number)
FROM numbers(2) AS l INNER JOIN numbers(2) AS r ON l.number = r.number
GROUP BY l.number; -- { serverError BAD_ARGUMENTS }

SELECT l.number, grouping(l.number)
FROM numbers(2) AS l INNER JOIN numbers(2) AS r ON l.number = r.number
GROUP BY l.number
ORDER BY l.number;

SELECT 'column sources inside of the key';

-- The lambda and the subquery are different nodes in the key and in the projection, and they are compared by content.
SELECT arrayMap(x -> x + l.number, [1])
FROM numbers(2) AS l INNER JOIN numbers(2) AS r ON l.number = r.number
GROUP BY arrayMap(x -> x + l.number, [1])
ORDER BY 1;

SELECT arrayMap(x -> x + r.number, [1])
FROM numbers(2) AS l INNER JOIN numbers(2) AS r ON l.number = r.number
GROUP BY arrayMap(x -> x + l.number, [1]); -- { serverError NOT_AN_AGGREGATE }

SELECT l.number IN (SELECT number FROM numbers(1))
FROM numbers(2) AS l INNER JOIN numbers(2) AS r ON l.number = r.number
GROUP BY l.number IN (SELECT number FROM numbers(1))
ORDER BY 1;

SELECT r.number IN (SELECT number FROM numbers(1))
FROM numbers(2) AS l INNER JOIN numbers(2) AS r ON l.number = r.number
GROUP BY l.number IN (SELECT number FROM numbers(1)); -- { serverError NOT_AN_AGGREGATE }

SELECT arrayMap(x -> x + l.number, [1])
FROM numbers(2) AS l INNER JOIN numbers(2) AS r ON l.number = r.number
GROUP BY arrayMap(x -> x + l.number, [1]) WITH ROLLUP
ORDER BY 1
SETTINGS group_by_use_nulls = 1;

DROP TABLE t_self_join;
