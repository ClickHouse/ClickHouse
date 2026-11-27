-- A row added by WITH FILL must show the filled or interpolated value when a pass rewrites the projection (#126873).
SELECT uniq(a) AS x FROM (SELECT number AS a, max(intDiv(number, 2) * 2) AS mb FROM numbers(4) GROUP BY a)
GROUP BY mb ORDER BY mb WITH FILL INTERPOLATE (x AS x + 100)
SETTINGS optimize_uniq_to_count = 1;

-- A LIMIT AFTER boundary over the same expression sees the interpolated values too.
SELECT uniq(a) AS x FROM (SELECT number AS a, max(intDiv(number, 2) * 4) AS mb FROM numbers(6) GROUP BY a)
GROUP BY mb ORDER BY mb WITH FILL INTERPOLATE (x AS x + 100) LIMIT AFTER uniq(a) > 100
SETTINGS optimize_uniq_to_count = 1;

-- The WITH FILL key is the rewritten column: by alias, by expression, by position, as a sorting prefix, in a tuple.
SELECT uniq(a) AS x FROM (SELECT number AS a FROM numbers(3) GROUP BY a) ORDER BY x WITH FILL FROM 0 TO 5
SETTINGS optimize_uniq_to_count = 1;
SELECT uniq(a) AS x FROM (SELECT number AS a FROM numbers(3) GROUP BY a) ORDER BY uniq(a) WITH FILL FROM 0 TO 5 SETTINGS optimize_uniq_to_count = 1;
SELECT uniq(a) AS x FROM (SELECT number AS a FROM numbers(3) GROUP BY a) ORDER BY 1 WITH FILL FROM 0 TO 5 SETTINGS optimize_uniq_to_count = 1;
SELECT uniq(a) AS x FROM (SELECT number AS a, any(number % 2) AS g FROM numbers(6) GROUP BY a) GROUP BY g ORDER BY x, g WITH FILL FROM 0 TO 3 SETTINGS optimize_uniq_to_count = 1;
SELECT uniq(a) AS x FROM (SELECT number AS a, any(number % 2) AS g FROM numbers(6) GROUP BY a) GROUP BY g ORDER BY (x, 1), g WITH FILL FROM 0 TO 3 SETTINGS optimize_uniq_to_count = 1;
SELECT tuple(a, b) AS t, a, b, g FROM values('a UInt64, b UInt64, g UInt64', (1, 2, 0), (1, 2, 2)) ORDER BY t, g WITH FILL;
SELECT x FROM (SELECT uniq(a) AS u FROM (SELECT number AS a FROM numbers(3) GROUP BY a) ORDER BY u WITH FILL FROM 0 TO 5) AS s(x) SETTINGS optimize_uniq_to_count = 1;

-- The WITH FILL level is still rewritten.
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT uniq(a) AS x FROM (SELECT number AS a, max(intDiv(number, 2) * 2) AS mb FROM numbers(4) GROUP BY a)
GROUP BY mb ORDER BY mb WITH FILL INTERPOLATE (x AS x + 100)) WHERE explain LIKE '%function_name: count%' SETTINGS optimize_uniq_to_count = 1;

-- WITH FILL in the subquery adds rows that repeat a value of the uniq argument.
SELECT uniq(a) FROM (SELECT a, max(b) AS m FROM values('a UInt64, b UInt64', (1, 0), (2, 2)) GROUP BY a ORDER BY m WITH FILL FROM 0 TO 5)
SETTINGS optimize_uniq_to_count = 1;
SELECT uniq(a) FROM (SELECT DISTINCT a FROM values('a UInt64, b UInt64', (1, 0), (2, 2)) ORDER BY b WITH FILL FROM 0 TO 5)
SETTINGS optimize_uniq_to_count = 1;

-- arrayJoin after GROUP BY in the subquery, or in the outer query, repeats rows.
SELECT uniq(a) FROM (SELECT a, arrayJoin([1, 2]) AS z FROM values('a UInt64', (1), (2)) GROUP BY a) SETTINGS optimize_uniq_to_count = 1;
SELECT uniq(a) FROM (SELECT a FROM values('a UInt64', (1), (2)) GROUP BY a ORDER BY arrayJoin([1, 2])) SETTINGS optimize_uniq_to_count = 1;
SELECT uniq(a) FROM (SELECT a FROM values('a UInt64', (1), (2)) GROUP BY a HAVING arrayJoin([1, 2]) > 0) SETTINGS optimize_uniq_to_count = 1;
SELECT uniq(a) FROM (SELECT a FROM values('a UInt64', (1), (2)) GROUP BY a QUALIFY arrayJoin([1, 2]) > 0) SETTINGS optimize_uniq_to_count = 1;
SELECT uniq(a) FROM (SELECT a FROM values('a UInt64', (1), (2)) GROUP BY a LIMIT 2 BY arrayJoin([1, 2])) SETTINGS optimize_uniq_to_count = 1;
SELECT uniq(a) FROM (SELECT a FROM values('a UInt64', (1), (2)) GROUP BY a) WHERE arrayJoin([1, 2]) > 0 SETTINGS optimize_uniq_to_count = 1;

-- An ARRAY JOIN clause runs before GROUP BY in the subquery, and in the outer query the join tree is not a subquery.
SELECT uniq(a) FROM (SELECT a FROM (SELECT [1, 1, 2] AS arr) ARRAY JOIN arr AS a GROUP BY a) SETTINGS optimize_uniq_to_count = 1;
SELECT uniq(a) FROM (SELECT a FROM values('a UInt64', (1), (2)) GROUP BY a) ARRAY JOIN [1, 2] AS z SETTINGS optimize_uniq_to_count = 1;

-- The subquery check needs WITH FILL: a subquery with a plain ORDER BY is still rewritten.
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT 1 AS k, c FROM (SELECT uniq(a) AS c FROM (SELECT number AS a FROM numbers(5) GROUP BY a ORDER BY a) ORDER BY c)
ORDER BY k WITH FILL FROM 0 TO 3 INTERPOLATE (c AS c + 1)) WHERE explain LIKE '%function_name: count%'
SETTINGS optimize_uniq_to_count = 1;

-- Only the outputs equal to the key take its values, also when two outputs have the same name.
SELECT a, * REPLACE (b AS a) FROM (SELECT number AS a, number * 2 AS b FROM numbers(2)) ORDER BY b WITH FILL;

-- Same-named columns of a self-join are different outputs.
SELECT t1.number AS x, t2.number AS y FROM numbers(2) AS t1, numbers(2) AS t2 ORDER BY x WITH FILL, y;

-- The IN rewrite replaces the subquery's projection with one tuple.
SELECT (number, number * 2) IN (SELECT a, a * 2 AS b FROM values('a UInt64', (0), (1)) ORDER BY a WITH FILL) FROM numbers(2)
SETTINGS rewrite_in_to_join = 1, allow_correlated_subqueries = 1;

-- Subqueries that differ only in the columns that take the filled values are different sets, in either order.
SELECT number, number IN (SELECT uniq(a) AS x FROM (SELECT number AS a FROM numbers(3) GROUP BY a) ORDER BY uniq(a) WITH FILL FROM 0 TO 5),
    number IN (SELECT count() AS x FROM (SELECT number AS a FROM numbers(3) GROUP BY a) ORDER BY uniq(a) WITH FILL FROM 0 TO 5)
FROM numbers(5) SETTINGS optimize_uniq_to_count = 1;
SELECT number, number IN (SELECT count() AS x FROM (SELECT number AS a FROM numbers(3) GROUP BY a) ORDER BY uniq(a) WITH FILL FROM 0 TO 5),
    number IN (SELECT uniq(a) AS x FROM (SELECT number AS a FROM numbers(3) GROUP BY a) ORDER BY uniq(a) WITH FILL FROM 0 TO 5)
FROM numbers(5) SETTINGS optimize_uniq_to_count = 1;
SELECT (9, 0) IN (SELECT number AS x, number AS y FROM numbers(2) ORDER BY number * 2 WITH FILL INTERPOLATE (x AS 9)),
    (9, 0) IN (SELECT number AS x, number AS y FROM numbers(2) ORDER BY number * 2 WITH FILL INTERPOLATE (y AS 9));
SELECT (9, 0) IN (SELECT number AS x, number AS y FROM numbers(2) ORDER BY number * 2 WITH FILL INTERPOLATE (y AS 9)),
    (9, 0) IN (SELECT number AS x, number AS y FROM numbers(2) ORDER BY number * 2 WITH FILL INTERPOLATE (x AS 9));

-- The subquery is sent to a shard.
DROP TABLE IF EXISTS t_with_fill_remote;
CREATE TABLE t_with_fill_remote (a UInt64) ENGINE = Memory;
INSERT INTO t_with_fill_remote VALUES (0), (1), (2);
SELECT a FROM remote('127.0.0.1', currentDatabase(), t_with_fill_remote)
WHERE a IN (SELECT uniq(a) AS x FROM (SELECT a FROM t_with_fill_remote GROUP BY a) ORDER BY uniq(a) WITH FILL FROM 0 TO 5) ORDER BY a SETTINGS optimize_uniq_to_count = 1;
SELECT a FROM remote('127.0.0.1', currentDatabase(), t_with_fill_remote)
WHERE a IN (SELECT uniq(a) AS x FROM (SELECT a FROM t_with_fill_remote GROUP BY a) ORDER BY 1 WITH FILL FROM 0 TO 5) ORDER BY a SETTINGS optimize_uniq_to_count = 1;

DROP TABLE IF EXISTS t_interpolate_substitute;
CREATE TABLE t_interpolate_substitute (k UInt64, a UInt64, b UInt64, CONSTRAINT c ASSUME a = b) ENGINE = MergeTree ORDER BY b;
INSERT INTO t_interpolate_substitute VALUES (0, 5, 5), (2, 7, 7);
SELECT k, max(a) AS x FROM t_interpolate_substitute WHERE k < 100 GROUP BY k ORDER BY k WITH FILL INTERPOLATE (x AS x + 100)
SETTINGS convert_query_to_cnf = 1, optimize_substitute_columns = 1;
SELECT a AS x FROM t_interpolate_substitute WHERE k < 100 ORDER BY x WITH FILL FROM 5 TO 8
SETTINGS convert_query_to_cnf = 1, optimize_substitute_columns = 1;
SELECT a AS x, a AS y FROM t_interpolate_substitute WHERE k < 100 ORDER BY x WITH FILL FROM 5 TO 8 SETTINGS convert_query_to_cnf = 1, optimize_substitute_columns = 1;
SELECT x FROM (SELECT k, a AS x FROM t_interpolate_substitute WHERE k < 100 ORDER BY x WITH FILL FROM 5 TO 8) SETTINGS convert_query_to_cnf = 1, optimize_substitute_columns = 1;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT a AS x FROM t_interpolate_substitute WHERE k < 100 ORDER BY x WITH FILL FROM 5 TO 8) WHERE explain LIKE '%column_name: b,%' SETTINGS convert_query_to_cnf = 1, optimize_substitute_columns = 1;
SELECT t1.k AS x, t2.k AS y FROM t_interpolate_substitute AS t1, t_interpolate_substitute AS t2 ORDER BY x WITH FILL, y;
SELECT a FROM remote('127.0.0.1', currentDatabase(), t_with_fill_remote)
WHERE a + 5 IN (SELECT a AS x FROM t_interpolate_substitute WHERE k < 100 ORDER BY x WITH FILL FROM 5 TO 8) ORDER BY a
SETTINGS convert_query_to_cnf = 1, optimize_substitute_columns = 1;
DROP TABLE t_interpolate_substitute;
DROP TABLE t_with_fill_remote;
