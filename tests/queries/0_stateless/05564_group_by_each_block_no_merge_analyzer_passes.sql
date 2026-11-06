-- `group_by_each_block_no_merge` can emit the same grouping key once per input block, so the analyzer passes
-- that rely on the keys being unique after `GROUP BY` must not apply to it. Each query runs with the
-- optimization on and off, and both results must be the same.

SET enable_analyzer = 1;

-- `optimize_uniq_to_count` must not turn the outer `uniqExact` into `count`: there are two distinct keys,
-- but each of the ten input blocks emits both of them.
SELECT uniqExact(k)
FROM
(
    SELECT number % 2 AS k FROM numbers(100) GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 10, max_threads = 1
)
SETTINGS optimize_uniq_to_count = 1, count_distinct_optimization = 0;

SELECT uniqExact(k)
FROM
(
    SELECT number % 2 AS k FROM numbers(100) GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 10, max_threads = 1
)
SETTINGS optimize_uniq_to_count = 0, count_distinct_optimization = 0;

-- `count_distinct_optimization` must not generate a `GROUP BY` subquery that inherits the streaming mode:
-- the query itself has no grouping keys, so its result must be exact.
SELECT uniqExact(k)
FROM (SELECT number % 2 AS k FROM numbers(100))
SETTINGS group_by_each_block_no_merge = 1, count_distinct_optimization = 1, optimize_uniq_to_count = 0,
    max_block_size = 10, max_threads = 1;

SELECT uniqExact(k)
FROM (SELECT number % 2 AS k FROM numbers(100))
SETTINGS group_by_each_block_no_merge = 1, count_distinct_optimization = 0, optimize_uniq_to_count = 0,
    max_block_size = 10, max_threads = 1;

-- `optimize_truncate_order_by_after_group_by_keys` must keep `s DESC`: the three input blocks emit
-- the rows (0, 0), (0, 2), (0, 4) for the key 0 and (1, 1), (1, 3), (1, 5) for the key 1.
SELECT number % 2 AS k, sum(number) AS s
FROM numbers(6)
GROUP BY k
ORDER BY k, s DESC
SETTINGS group_by_each_block_no_merge = 1, max_block_size = 2, max_threads = 1,
    optimize_truncate_order_by_after_group_by_keys = 1;

SELECT number % 2 AS k, sum(number) AS s
FROM numbers(6)
GROUP BY k
ORDER BY k, s DESC
SETTINGS group_by_each_block_no_merge = 1, max_block_size = 2, max_threads = 1,
    optimize_truncate_order_by_after_group_by_keys = 0;
