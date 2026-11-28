-- `group_by_each_block_no_merge` can emit the same grouping key once per input block, so the plan
-- optimizations that rely on the keys being unique after an aggregation must not apply to it.

-- The outer `DISTINCT` must stay: each of the ten input blocks emits (0, 5) and (1, 5).
SELECT count()
FROM
(
    SELECT DISTINCT k, c
    FROM
    (
        SELECT number % 2 AS k, count() AS c
        FROM numbers(100) GROUP BY k
        SETTINGS group_by_each_block_no_merge = 1, max_block_size = 10, max_threads = 1
    )
)
SETTINGS query_plan_remove_redundant_distinct = 1;

SELECT count()
FROM
(
    SELECT DISTINCT k, c
    FROM
    (
        SELECT number % 2 AS k, count() AS c
        FROM numbers(100) GROUP BY k
        SETTINGS group_by_each_block_no_merge = 1, max_block_size = 10, max_threads = 1
    )
)
SETTINGS query_plan_remove_redundant_distinct = 0;

-- `LEFT ANY JOIN` must still pick one of the ten matching rows instead of being converted to `INNER ALL JOIN`.
SELECT count()
FROM numbers(1) AS l
LEFT ANY JOIN
(
    SELECT number % 2 + 1 AS k, count()::Nullable(UInt64) AS c
    FROM numbers(100) GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 10, max_threads = 1
) AS r ON l.number + 1 = r.k
WHERE l.number < r.c
SETTINGS query_plan_convert_outer_join_to_inner_join = 1,
    query_plan_filter_push_down = 0,
    query_plan_convert_any_join_to_semi_or_anti_join = 0,
    query_plan_join_swap_table = 0,
    enable_join_runtime_filters = 0;

SELECT count()
FROM numbers(1) AS l
LEFT ANY JOIN
(
    SELECT number % 2 + 1 AS k, count()::Nullable(UInt64) AS c
    FROM numbers(100) GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 10, max_threads = 1
) AS r ON l.number + 1 = r.k
WHERE l.number < r.c
SETTINGS query_plan_convert_outer_join_to_inner_join = 0,
    query_plan_filter_push_down = 0,
    query_plan_convert_any_join_to_semi_or_anti_join = 0,
    query_plan_join_swap_table = 0,
    enable_join_runtime_filters = 0;
