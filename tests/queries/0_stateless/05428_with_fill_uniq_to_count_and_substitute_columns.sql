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

-- The skip is per query level: a subquery without WITH FILL is still rewritten.
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT 1 AS k, c FROM (SELECT uniq(a) AS c FROM (SELECT number AS a FROM numbers(5) GROUP BY a))
ORDER BY k WITH FILL FROM 0 TO 3 INTERPOLATE (c AS c + 1)) WHERE explain LIKE '%function_name: count%'
SETTINGS optimize_uniq_to_count = 1;

DROP TABLE IF EXISTS t_interpolate_substitute;
CREATE TABLE t_interpolate_substitute (k UInt64, a UInt64, b UInt64, CONSTRAINT c ASSUME a = b) ENGINE = MergeTree ORDER BY b;
INSERT INTO t_interpolate_substitute VALUES (0, 5, 5), (2, 7, 7);
SELECT k, max(a) AS x FROM t_interpolate_substitute WHERE k < 100 GROUP BY k ORDER BY k WITH FILL INTERPOLATE (x AS x + 100)
SETTINGS convert_query_to_cnf = 1, optimize_substitute_columns = 1;
SELECT a AS x FROM t_interpolate_substitute WHERE k < 100 ORDER BY x WITH FILL FROM 5 TO 8
SETTINGS convert_query_to_cnf = 1, optimize_substitute_columns = 1;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT k, x FROM (SELECT k, max(a) AS x FROM t_interpolate_substitute WHERE k < 100 GROUP BY k)
ORDER BY k WITH FILL INTERPOLATE (x AS x + 100)) WHERE explain LIKE '%column_name: b,%'
SETTINGS convert_query_to_cnf = 1, optimize_substitute_columns = 1;
DROP TABLE t_interpolate_substitute;
