-- `WITH ROLLUP` merges a level with many rows on `max_threads` threads, split by the hash of the level's keys.
-- With `max_threads = 1` every level is merged on one thread, so the results must be the same.

DROP TABLE IF EXISTS t_parallel_rollup;
CREATE TABLE t_parallel_rollup (a UInt32, b Nullable(String), c LowCardinality(String), d UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
-- 300000 distinct (a, b, c, d), so the first levels have more rows than the parallel merge needs.
INSERT INTO t_parallel_rollup SELECT number % 13, if(number % 17 = 0, NULL, toString(number % 101)), toString(number % 7), number % 300000, number FROM numbers(600000);

SET max_block_size = 10000;

SELECT 'rollup';
SELECT count(), sum(cityHash64(a, b, c, d, s, n, g)) FROM
(
    SELECT a, b, c, d, sum(v) AS s, count() AS n, GROUPING(a, b, c, d) AS g FROM t_parallel_rollup GROUP BY a, b, c, d WITH ROLLUP
) SETTINGS max_threads = 1;
SELECT count(), sum(cityHash64(a, b, c, d, s, n, g)) FROM
(
    SELECT a, b, c, d, sum(v) AS s, count() AS n, GROUPING(a, b, c, d) AS g FROM t_parallel_rollup GROUP BY a, b, c, d WITH ROLLUP
) SETTINGS max_threads = 8;

SELECT 'group_by_use_nulls';
SELECT count(), sum(cityHash64(a, b, c, d, s)) FROM
(
    SELECT a, b, c, d, sum(v) AS s FROM t_parallel_rollup GROUP BY ROLLUP(a, b, c, d)
) SETTINGS max_threads = 1, group_by_use_nulls = 1;
SELECT count(), sum(cityHash64(a, b, c, d, s)) FROM
(
    SELECT a, b, c, d, sum(v) AS s FROM t_parallel_rollup GROUP BY ROLLUP(a, b, c, d)
) SETTINGS max_threads = 8, group_by_use_nulls = 1;

SELECT 'uniqExact and HAVING';
SELECT count(), sum(cityHash64(d, c, u)) FROM
(
    SELECT d, c, uniqExact(v % 1000) AS u FROM t_parallel_rollup GROUP BY d, c WITH ROLLUP HAVING count() > 1
) SETTINGS max_threads = 1;
SELECT count(), sum(cityHash64(d, c, u)) FROM
(
    SELECT d, c, uniqExact(v % 1000) AS u FROM t_parallel_rollup GROUP BY d, c WITH ROLLUP HAVING count() > 1
) SETTINGS max_threads = 8;

SELECT 'rows per level';
SELECT g, count() FROM
(
    SELECT GROUPING(a, b, c, d) AS g FROM t_parallel_rollup GROUP BY a, b, c, d WITH ROLLUP
) GROUP BY g ORDER BY g SETTINGS max_threads = 8;

DROP TABLE t_parallel_rollup;
