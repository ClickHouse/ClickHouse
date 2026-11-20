-- `additional_result_filter` must not reorder the result columns, also when the filter skips over a column:
-- with header [a, b, c] and filter `c > a` the filter DAG references only `a` and `c`, and `b` must stay in place.

DROP TABLE IF EXISTS t_05398;
CREATE TABLE t_05398 (a UInt8, b UInt8, c UInt8) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_05398 VALUES (1, 2, 3), (5, 4, 3);

SELECT * FROM t_05398 ORDER BY a FORMAT TSVWithNames SETTINGS additional_result_filter = 'c > a';
SELECT * FROM t_05398 ORDER BY a FORMAT TSVWithNames SETTINGS additional_result_filter = 'b > a';
SELECT c, b, a FROM t_05398 ORDER BY a FORMAT TSVWithNames SETTINGS additional_result_filter = 'a < c';
SELECT a + 1 AS x, b, c FROM t_05398 ORDER BY a FORMAT TSVWithNames SETTINGS additional_result_filter = 'c > x';
SELECT * FROM (SELECT * FROM t_05398 UNION ALL SELECT * FROM t_05398) ORDER BY a FORMAT TSVWithNames SETTINGS additional_result_filter = 'c > a';

-- A filter that is a column itself keeps that column.
SELECT a, b FROM t_05398 ORDER BY a FORMAT TSVWithNames SETTINGS additional_result_filter = 'b';

DROP TABLE t_05398;
