-- A row added by WITH FILL must show the filled or interpolated value when a pass rewrites the projection (#126873).
SELECT uniq(a) AS x FROM (SELECT number AS a, max(intDiv(number, 2) * 2) AS mb FROM numbers(4) GROUP BY a)
GROUP BY mb ORDER BY mb WITH FILL INTERPOLATE (x AS x + 100)
SETTINGS optimize_uniq_to_count = 1;

-- A LIMIT AFTER boundary over the same expression sees the interpolated values too.
SELECT uniq(a) AS x FROM (SELECT number AS a, max(intDiv(number, 2) * 4) AS mb FROM numbers(6) GROUP BY a)
GROUP BY mb ORDER BY mb WITH FILL INTERPOLATE (x AS x + 100) LIMIT AFTER uniq(a) > 100
SETTINGS optimize_uniq_to_count = 1;

-- Without INTERPOLATE, the WITH FILL key is a copy of the rewritten column.
SELECT uniq(a) AS x FROM (SELECT number AS a FROM numbers(3) GROUP BY a) ORDER BY x WITH FILL FROM 0 TO 5
SETTINGS optimize_uniq_to_count = 1;

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

-- The skip is per query level and needs WITH FILL: a subquery with a plain ORDER BY is still rewritten.
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT 1 AS k, c FROM (SELECT uniq(a) AS c FROM (SELECT number AS a FROM numbers(5) GROUP BY a ORDER BY a) ORDER BY c)
ORDER BY k WITH FILL FROM 0 TO 3 INTERPOLATE (c AS c + 1)) WHERE explain LIKE '%function_name: count%'
SETTINGS optimize_uniq_to_count = 1;

DROP TABLE IF EXISTS t_interpolate_substitute;
CREATE TABLE t_interpolate_substitute (k UInt64, a UInt64, b UInt64, CONSTRAINT c ASSUME a = b) ENGINE = MergeTree ORDER BY b;
INSERT INTO t_interpolate_substitute VALUES (0, 5, 5), (2, 7, 7);
SELECT k, max(a) AS x FROM t_interpolate_substitute WHERE k < 100 GROUP BY k ORDER BY k WITH FILL INTERPOLATE (x AS x + 100)
SETTINGS convert_query_to_cnf = 1, optimize_substitute_columns = 1;
SELECT a AS x FROM t_interpolate_substitute WHERE k < 100 ORDER BY x WITH FILL FROM 5 TO 8
SETTINGS convert_query_to_cnf = 1, optimize_substitute_columns = 1;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT k, x FROM (SELECT k, max(a) AS x FROM t_interpolate_substitute WHERE k < 100 GROUP BY k ORDER BY k)
ORDER BY k WITH FILL INTERPOLATE (x AS x + 100)) WHERE explain LIKE '%column_name: b,%'
SETTINGS convert_query_to_cnf = 1, optimize_substitute_columns = 1;
DROP TABLE t_interpolate_substitute;
