-- `WITH ROLLUP` merges a level with many rows on `max_threads` threads, split by the hash of the level's keys.
-- With `max_threads = 1` every level is merged on one thread, so the results must be the same.

DROP TABLE IF EXISTS t_parallel_rollup;
CREATE TABLE t_parallel_rollup (a UInt32, b Nullable(String), c LowCardinality(String), d UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
-- 120000 distinct (a, b, c, d): more rows in the first level than the parallel merge needs (100000).
INSERT INTO t_parallel_rollup SELECT number % 13, if(number % 17 = 0, NULL, toString(number % 101)), toString(number % 7), number % 60000, number FROM numbers(120000);

SET max_block_size = 10000;

-- Rows and a hash of each level.
SELECT 'rollup';
SELECT g, count(), sum(cityHash64(toString((a, b, c, d, s, n)))) FROM
(
    SELECT a, b, c, d, sum(v) AS s, count() AS n, GROUPING(a, b, c, d) AS g FROM t_parallel_rollup GROUP BY a, b, c, d WITH ROLLUP
) GROUP BY g ORDER BY g SETTINGS max_threads = 1;
SELECT g, count(), sum(cityHash64(toString((a, b, c, d, s, n)))) FROM
(
    SELECT a, b, c, d, sum(v) AS s, count() AS n, GROUPING(a, b, c, d) AS g FROM t_parallel_rollup GROUP BY a, b, c, d WITH ROLLUP
) GROUP BY g ORDER BY g SETTINGS max_threads = 8;

-- `group_by_use_nulls` merges the levels with the aggregator of the intermediate header.
SELECT 'group_by_use_nulls';
SELECT count(), sum(cityHash64(toString((a, b, c, d, s)))) FROM
(
    SELECT a, b, c, d, sum(v) AS s FROM t_parallel_rollup GROUP BY ROLLUP(a, b, c, d)
) SETTINGS max_threads = 1, group_by_use_nulls = 1;
SELECT count(), sum(cityHash64(toString((a, b, c, d, s)))) FROM
(
    SELECT a, b, c, d, sum(v) AS s FROM t_parallel_rollup GROUP BY ROLLUP(a, b, c, d)
) SETTINGS max_threads = 8, group_by_use_nulls = 1;

DROP TABLE t_parallel_rollup;
