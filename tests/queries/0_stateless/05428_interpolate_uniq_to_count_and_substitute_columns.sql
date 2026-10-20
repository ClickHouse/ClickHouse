-- An INTERPOLATE target must keep reading the interpolated values when a pass rewrites the projection (#126873).
SELECT uniq(a) AS x FROM (SELECT number AS a, max(intDiv(number, 2) * 2) AS mb FROM numbers(4) GROUP BY a)
GROUP BY mb ORDER BY mb WITH FILL INTERPOLATE (x AS x + 100)
SETTINGS optimize_uniq_to_count = 1;

-- A LIMIT AFTER boundary over the same expression sees the interpolated values too.
SELECT uniq(a) AS x FROM (SELECT number AS a, max(intDiv(number, 2) * 4) AS mb FROM numbers(6) GROUP BY a)
GROUP BY mb ORDER BY mb WITH FILL INTERPOLATE (x AS x + 100) LIMIT AFTER uniq(a) > 100
SETTINGS optimize_uniq_to_count = 1;

DROP TABLE IF EXISTS t_interpolate_substitute;
CREATE TABLE t_interpolate_substitute (k UInt64, a UInt64, b UInt64, CONSTRAINT c ASSUME a = b) ENGINE = MergeTree ORDER BY b;
INSERT INTO t_interpolate_substitute VALUES (0, 5, 5), (2, 7, 7);
SELECT k, max(a) AS x FROM t_interpolate_substitute WHERE k < 100 GROUP BY k ORDER BY k WITH FILL INTERPOLATE (x AS x + 100)
SETTINGS convert_query_to_cnf = 1, optimize_substitute_columns = 1;
DROP TABLE t_interpolate_substitute;
