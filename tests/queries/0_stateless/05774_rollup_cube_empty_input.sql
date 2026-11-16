-- https://github.com/ClickHouse/ClickHouse/issues/131974
-- GROUP BY ROLLUP / CUBE over an empty input must still produce the grand-total row,
-- like the equivalent GROUPING SETS query does (aggregation over the empty set).

DROP TABLE IF EXISTS t_empty_rollup;
CREATE TABLE t_empty_rollup (id Int32, c0 Nullable(Int32)) ENGINE = Memory;

-- Grand-total row only: c0 = NULL, grouping = 1, count() = 0.
SELECT c0, grouping(c0), count() FROM t_empty_rollup GROUP BY ROLLUP(c0);
SELECT c0, grouping(c0), count() FROM t_empty_rollup GROUP BY CUBE(c0);
SELECT count() FROM (SELECT c0, count() AS n FROM t_empty_rollup GROUP BY ROLLUP(c0));
SELECT count() FROM (SELECT c0, count() AS n FROM t_empty_rollup GROUP BY GROUPING SETS ((c0), ()));

-- Same, with NULL keys instead of defaults (group_by_use_nulls).
SELECT c0, count() FROM t_empty_rollup GROUP BY ROLLUP(c0) SETTINGS group_by_use_nulls = 1;

-- Sanity check: non-empty input output is unchanged (finest level + grand total).
INSERT INTO t_empty_rollup VALUES (1, 10), (2, 20), (3, NULL);
SELECT c0, grouping(c0), count() FROM t_empty_rollup GROUP BY ROLLUP(c0) ORDER BY grouping(c0), c0 NULLS FIRST;
SELECT c0, grouping(c0), count() FROM t_empty_rollup GROUP BY CUBE(c0) ORDER BY grouping(c0), c0 NULLS FIRST;

DROP TABLE t_empty_rollup;
