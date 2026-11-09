-- A `-Merge` window function of `contingency` and its relatives keeps its own state in the window layout and
-- merges ordinary states into it, through each combinator between `-Merge` and the function. `-MergeState`
-- builds the same merge with the ordinary layout, so the two must give the same values on every row.
-- The values are compared as text, so `nan` equals `nan`, and `+ 0` turns `-0` into `0`.

DROP TABLE IF EXISTS t_crosstab_window_merge;
CREATE TABLE t_crosstab_window_merge (p UInt32, k UInt32, b UInt32, c UInt32, n Nullable(UInt32))
ENGINE = MergeTree ORDER BY (p, k);
INSERT INTO t_crosstab_window_merge
SELECT number % 3, intDiv(number, 3) % 20, number % 4, intDiv(number, 7) % 5, if(number % 11 = 0, NULL, number % 4)
FROM numbers(600);

SELECT '-- each row: window layout equals ordinary layout';
SELECT
    count(),
    countIf(toString(plain) != toString(plain_ordinary)),
    countIf(toString(cramers) != toString(cramers_ordinary)),
    countIf(toString(cramers_bc) != toString(cramers_bc_ordinary)),
    countIf(toString(theils) != toString(theils_ordinary)),
    countIf(toString(if_merge) != toString(if_ordinary)),
    countIf(toString(array_merge) != toString(array_ordinary)),
    countIf(toString(for_each) != toString(for_each_ordinary)),
    countIf(toString(nullable) != toString(nullable_ordinary)),
    countIf(toString(or_null) != toString(or_null_ordinary)),
    countIf(toString(resample) != toString(resample_ordinary)),
    countIf(toString(distinct) != toString(distinct_ordinary)),
    countIf(toString(tuple_merge) != toString(tuple_ordinary)),
    countIf(toString(arg_max) != toString(arg_max_ordinary)),
    countIf(toString(map_merge) != toString(map_ordinary))
FROM
(
    SELECT
        round(contingencyMerge(s_plain) OVER w, 6) + 0 AS plain,
        round(finalizeAggregation(contingencyMergeState(s_plain) OVER w), 6) + 0 AS plain_ordinary,
        round(cramersVMerge(s_cramers) OVER w, 6) + 0 AS cramers,
        round(finalizeAggregation(cramersVMergeState(s_cramers) OVER w), 6) + 0 AS cramers_ordinary,
        round(cramersVBiasCorrectedMerge(s_cramers_bc) OVER w, 6) + 0 AS cramers_bc,
        round(finalizeAggregation(cramersVBiasCorrectedMergeState(s_cramers_bc) OVER w), 6) + 0 AS cramers_bc_ordinary,
        round(theilsUMerge(s_theils) OVER w, 6) + 0 AS theils,
        round(finalizeAggregation(theilsUMergeState(s_theils) OVER w), 6) + 0 AS theils_ordinary,
        round(contingencyIfMerge(s_if) OVER w, 6) + 0 AS if_merge,
        round(finalizeAggregation(contingencyIfMergeState(s_if) OVER w), 6) + 0 AS if_ordinary,
        round(contingencyArrayMerge(s_array) OVER w, 6) + 0 AS array_merge,
        round(finalizeAggregation(contingencyArrayMergeState(s_array) OVER w), 6) + 0 AS array_ordinary,
        arrayMap(x -> round(x, 6) + 0, contingencyForEachMerge(s_for_each) OVER w) AS for_each,
        arrayMap(x -> round(x, 6) + 0, finalizeAggregation(contingencyForEachMergeState(s_for_each) OVER w)) AS for_each_ordinary,
        round(contingencyMerge(s_nullable) OVER w, 6) + 0 AS nullable,
        round(finalizeAggregation(contingencyMergeState(s_nullable) OVER w), 6) + 0 AS nullable_ordinary,
        round(contingencyOrNullMerge(s_or_null) OVER w, 6) + 0 AS or_null,
        round(finalizeAggregation(contingencyOrNullMergeState(s_or_null) OVER w), 6) + 0 AS or_null_ordinary,
        arrayMap(x -> round(x, 6) + 0, contingencyResampleMerge(0, 20, 10)(s_resample) OVER w) AS resample,
        arrayMap(x -> round(x, 6) + 0, finalizeAggregation(contingencyResampleMergeState(0, 20, 10)(s_resample) OVER w)) AS resample_ordinary,
        round(contingencyDistinctMerge(s_distinct) OVER w, 6) + 0 AS distinct,
        round(finalizeAggregation(contingencyDistinctMergeState(s_distinct) OVER w), 6) + 0 AS distinct_ordinary,
        round(tupleElement(contingencyTupleMerge(s_tuple) OVER w, 1), 6) + 0 AS tuple_merge,
        round(tupleElement(finalizeAggregation(contingencyTupleMergeState(s_tuple) OVER w), 1), 6) + 0 AS tuple_ordinary,
        round(contingencyArgMaxMerge(s_arg_max) OVER w, 6) + 0 AS arg_max,
        round(finalizeAggregation(contingencyArgMaxMergeState(s_arg_max) OVER w), 6) + 0 AS arg_max_ordinary,
        mapApply((key, value) -> (key, round(value, 6) + 0), contingencyMapMerge(s_map) OVER w) AS map_merge,
        mapApply((key, value) -> (key, round(value, 6) + 0), finalizeAggregation(contingencyMapMergeState(s_map) OVER w)) AS map_ordinary
    FROM
    (
        SELECT
            p,
            k,
            contingencyState(b, c) AS s_plain,
            cramersVState(b, c) AS s_cramers,
            cramersVBiasCorrectedState(b, c) AS s_cramers_bc,
            theilsUState(b, c) AS s_theils,
            contingencyIfState(b, c, b != 1) AS s_if,
            contingencyArrayState([b], [c]) AS s_array,
            contingencyForEachState([b, c], [c, b]) AS s_for_each,
            contingencyState(n, c) AS s_nullable,
            contingencyOrNullState(b, c) AS s_or_null,
            contingencyResampleState(0, 20, 10)(b, c, k) AS s_resample,
            contingencyDistinctState(b, c) AS s_distinct,
            contingencyTupleState(tuple(b), tuple(c)) AS s_tuple,
            contingencyArgMaxState(b, c, k) AS s_arg_max,
            contingencyMapState(b, c, p) AS s_map
        FROM t_crosstab_window_merge
        GROUP BY p, k
    )
    WINDOW w AS (PARTITION BY p ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
);

SELECT '-- whole partition: window merge, then the plain aggregate';
SELECT DISTINCT p, round(contingencyMerge(s) OVER (PARTITION BY p), 6), round((contingencyMapMerge(sm) OVER (PARTITION BY p))[p], 6)
FROM (SELECT p, contingencyState(b, c) AS s, contingencyMapState(b, c, p) AS sm FROM t_crosstab_window_merge GROUP BY p, k)
ORDER BY p;
SELECT p, round(contingency(b, c), 6), round(contingencyMap(b, c, p)[p], 6) FROM t_crosstab_window_merge GROUP BY p ORDER BY p;

DROP TABLE t_crosstab_window_merge;
