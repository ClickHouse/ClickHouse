-- https://github.com/ClickHouse/ClickHouse/issues/131974
-- GROUP BY ROLLUP / CUBE over an empty input must still produce the grand-total row,
-- like the equivalent GROUPING SETS query does (aggregation over the empty set).

DROP TABLE IF EXISTS t_empty_rollup;
DROP TABLE IF EXISTS t_empty_rollup_nonull;
CREATE TABLE t_empty_rollup (id Int32, c0 Nullable(Int32)) ENGINE = Memory;
CREATE TABLE t_empty_rollup_nonull (id Int32, c0 Int32) ENGINE = Memory;

-- Grand-total row only: c0 = NULL, grouping = 1, count() = 0.
SELECT c0, grouping(c0), count() FROM t_empty_rollup GROUP BY ROLLUP(c0);
SELECT c0, grouping(c0), count() FROM t_empty_rollup GROUP BY CUBE(c0);
SELECT count() FROM (SELECT c0, count() AS n FROM t_empty_rollup GROUP BY ROLLUP(c0));
SELECT count() FROM (SELECT c0, count() AS n FROM t_empty_rollup GROUP BY GROUPING SETS ((c0), ()));

-- Same over a non-Nullable key: the emitted row carries the key default value 0.
SELECT c0, grouping(c0), count() FROM t_empty_rollup_nonull GROUP BY ROLLUP(c0);
SELECT c0, grouping(c0), count() FROM t_empty_rollup_nonull GROUP BY CUBE(c0);

-- NULL keys (group_by_use_nulls) over Nullable and non-Nullable key types.
SELECT c0, grouping(c0), count() FROM t_empty_rollup GROUP BY ROLLUP(c0) SETTINGS group_by_use_nulls = 1;
SELECT c0, grouping(c0), count() FROM t_empty_rollup_nonull GROUP BY ROLLUP(c0) SETTINGS group_by_use_nulls = 1;

-- ROLLUP over an empty Distributed table (two shards pointing at the local server).
SELECT c0, grouping(c0), count() FROM remote('127.0.0.{1,2}', currentDatabase(), 't_empty_rollup') GROUP BY ROLLUP(c0);

-- With empty_result_for_aggregation_by_empty_set = 1 the empty grouping set produces no
-- row either, and ROLLUP/CUBE must match: no grand-total row.
SET empty_result_for_aggregation_by_empty_set = 1;
SELECT 'no rows expected' AS marker;
SELECT c0, grouping(c0), count() FROM t_empty_rollup GROUP BY ROLLUP(c0);
SELECT c0, grouping(c0), count() FROM t_empty_rollup GROUP BY CUBE(c0);
SELECT count() FROM (SELECT c0, count() AS n FROM t_empty_rollup GROUP BY GROUPING SETS ((c0), ()));
SET empty_result_for_aggregation_by_empty_set = 0;

-- Sanity check: non-empty input output is unchanged (finest level + grand total).
INSERT INTO t_empty_rollup VALUES (1, 10), (2, 20), (3, NULL);
SELECT c0, grouping(c0), count() FROM t_empty_rollup GROUP BY ROLLUP(c0) ORDER BY grouping(c0), c0 NULLS FIRST;
SELECT c0, grouping(c0), count() FROM t_empty_rollup GROUP BY CUBE(c0) ORDER BY grouping(c0), c0 NULLS FIRST;

DROP TABLE t_empty_rollup;
DROP TABLE t_empty_rollup_nonull;
