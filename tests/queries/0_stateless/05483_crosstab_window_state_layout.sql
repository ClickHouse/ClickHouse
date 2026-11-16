-- `contingency` and its relatives keep a window function's state in a separate memory layout. A state
-- that a window function returns with `-State` has the same type name as an ordinary state, so it must
-- use the ordinary layout: otherwise a spill, a `CAST` or a distributed plan reads one layout as the other.

DROP TABLE IF EXISTS t_crosstab_window_state;
CREATE TABLE t_crosstab_window_state (p UInt32, b UInt32, c UInt32, v UInt32) ENGINE = MergeTree ORDER BY (p, v)
SETTINGS index_granularity = 8;
INSERT INTO t_crosstab_window_state SELECT number % 4, number % 3, intDiv(number, 3) % 5, number FROM numbers(400);

SELECT '-- window states give the same values as ordinary states';
SELECT p, round(finalizeAggregation(any(s)), 6), round(contingencyMerge(s), 6)
FROM (SELECT p, contingencyState(b, c) OVER (PARTITION BY p) AS s FROM t_crosstab_window_state)
GROUP BY p ORDER BY p;
SELECT p, round(contingency(b, c), 6) FROM t_crosstab_window_state GROUP BY p ORDER BY p;
SELECT count()
FROM
(
    SELECT
        round(finalizeAggregation(contingencyState(b, c) OVER (ORDER BY v)), 6) AS from_state,
        round(contingency(b, c) OVER (ORDER BY v), 6) AS direct
    FROM t_crosstab_window_state
)
WHERE from_state != direct AND NOT (isNaN(from_state) AND isNaN(direct));

SELECT '-- window state through a spilled sort, in an Array and in a Tuple';
-- Without query_plan_enable_optimizations = 0 the Array and the Tuple are built above the sort.
SELECT sum(cityHash64(v, round(finalizeAggregation(a[1]), 6), round(finalizeAggregation(tp.1), 6)))
FROM
(
    SELECT v, [s] AS a, tuple(s, v) AS tp
    FROM (SELECT v, contingencyState(b, c) OVER (PARTITION BY p ORDER BY v) AS s FROM t_crosstab_window_state)
    ORDER BY v DESC
)
SETTINGS query_plan_enable_optimizations = 0, max_bytes_before_external_sort = 1000, max_bytes_ratio_before_external_sort = 0,
    max_block_size = 8;
SELECT sum(cityHash64(v, round(finalizeAggregation(a[1]), 6), round(finalizeAggregation(tp.1), 6)))
FROM
(
    SELECT v, [s] AS a, tuple(s, v) AS tp
    FROM (SELECT v, contingencyState(b, c) OVER (PARTITION BY p ORDER BY v) AS s FROM t_crosstab_window_state)
    ORDER BY v DESC
)
SETTINGS query_plan_enable_optimizations = 0, max_bytes_before_external_sort = 0;

SELECT '-- window state inside another state, cast to its type name';
SELECT round(finalizeAggregation(finalizeAggregation(CAST(anyState(tuple(s, CAST(1 AS Dynamic)))
    AS AggregateFunction(any, Tuple(AggregateFunction(contingency, UInt32, UInt32), Dynamic)))).1), 6)
FROM (SELECT contingencyState(b, c) OVER () AS s FROM t_crosstab_window_state);

SELECT '-- window state merged in a distributed plan executed locally';
SET max_rows_to_group_by = 0;
SELECT round(contingencyMerge(CAST(s AS AggregateFunction(contingency, UInt32, UInt32))), 6),
    round(cramersVMerge(CAST(s2 AS AggregateFunction(cramersV, UInt32, UInt32))), 6),
    round(theilsUMerge(s3), 6)
FROM
(
    SELECT contingencyState(b, c) OVER () AS s, cramersVState(b, c) OVER () AS s2, theilsUState(b, c) OVER () AS s3
    FROM t_crosstab_window_state LIMIT 1
)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_fallback_to_local_execution = 0;
SELECT round(contingency(b, c), 6), round(cramersV(b, c), 6), round(theilsU(b, c), 6) FROM t_crosstab_window_state;

DROP TABLE t_crosstab_window_state;
