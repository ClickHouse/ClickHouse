-- ANY INNER JOIN returns one row per key also when ON has a condition besides the key equality.

CREATE TABLE l (k UInt64, x UInt64) ENGINE = Memory;
CREATE TABLE r (k UInt64, y UInt64) ENGINE = Memory;
INSERT INTO l VALUES (1, 0), (1, 1);
INSERT INTO r VALUES (1, 2);

SELECT 'hash', l.x, r.y FROM l ANY INNER JOIN r ON l.k = r.k AND l.x < r.y ORDER BY ALL
SETTINGS join_algorithm = 'hash', max_threads = 1, query_plan_join_swap_table = 0, any_join_distinct_right_table_keys = 0;
SELECT 'parallel_hash', l.x, r.y FROM l ANY INNER JOIN r ON l.k = r.k AND l.x < r.y ORDER BY ALL
SETTINGS join_algorithm = 'parallel_hash', max_threads = 1, query_plan_join_swap_table = 0, any_join_distinct_right_table_keys = 0;
SELECT 'grace_hash', l.x, r.y FROM l ANY INNER JOIN r ON l.k = r.k AND l.x < r.y ORDER BY ALL
SETTINGS join_algorithm = 'grace_hash', max_threads = 1, query_plan_join_swap_table = 0, any_join_distinct_right_table_keys = 0;

-- Two right rows for the key, in both build orientations.
INSERT INTO r VALUES (1, 3);
SELECT 'two right rows', count() FROM l ANY INNER JOIN r ON l.k = r.k AND l.x < r.y
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, any_join_distinct_right_table_keys = 0;
SELECT 'two right rows, swapped', count() FROM l ANY INNER JOIN r ON l.k = r.k AND l.x < r.y
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 1, any_join_distinct_right_table_keys = 0;

-- Many left rows per key, probed by several threads.
CREATE TABLE l_big ENGINE = Memory AS SELECT number % 10 AS k, number AS x FROM numbers(100000);
CREATE TABLE r_small ENGINE = Memory AS SELECT number AS k, number AS y FROM numbers(10);
SELECT 'hash', count(), uniqExact(l_big.k) FROM l_big ANY INNER JOIN r_small ON l_big.k = r_small.k AND l_big.x > r_small.y
SETTINGS join_algorithm = 'hash', max_threads = 8, query_plan_join_swap_table = 0, any_join_distinct_right_table_keys = 0;
SELECT 'parallel_hash', count(), uniqExact(l_big.k) FROM l_big ANY INNER JOIN r_small ON l_big.k = r_small.k AND l_big.x > r_small.y
SETTINGS join_algorithm = 'parallel_hash', max_threads = 8, query_plan_join_swap_table = 0, any_join_distinct_right_table_keys = 0;
SELECT 'grace_hash', count(), uniqExact(l_big.k) FROM l_big ANY INNER JOIN r_small ON l_big.k = r_small.k AND l_big.x > r_small.y
SETTINGS join_algorithm = 'grace_hash', max_threads = 8, query_plan_join_swap_table = 0, any_join_distinct_right_table_keys = 0;

-- Unchanged: ANY LEFT keeps one row per left row, the legacy ANY INNER keeps every left row.
SELECT 'left any', count() FROM l ANY LEFT JOIN r ON l.k = r.k AND l.x < r.y
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, any_join_distinct_right_table_keys = 0;
SELECT 'legacy any inner', count() FROM l ANY INNER JOIN r ON l.k = r.k AND l.x < r.y
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, any_join_distinct_right_table_keys = 1;
