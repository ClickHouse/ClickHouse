-- States that a window function returns with `-State` have the ordinary layout, and a `-Merge` window
-- function merges them into its window-layout state through every combinator around the function:
-- `-If` and `-Array` around `-Tuple`, and `-Map` and `-ForEach` around another `-Merge`. `-MergeState`
-- builds the same merge with the ordinary layout, so the two must give the same values on every row.
-- The values are compared as text, so `nan` equals `nan`, and `+ 0` turns `-0` into `0`.

DROP TABLE IF EXISTS t_crosstab_window_wrapped;
CREATE TABLE t_crosstab_window_wrapped (p UInt32, k UInt32, b UInt32, c UInt32, d UInt32) ENGINE = MergeTree ORDER BY (p, k);
INSERT INTO t_crosstab_window_wrapped
SELECT number % 3, intDiv(number, 3) % 20, number % 4, intDiv(number, 7) % 5, intDiv(number, 11) % 3 FROM numbers(600);

SELECT '-- each row: window layout equals ordinary layout';
SELECT
    count(),
    countIf(toString(tuple_if) != toString(tuple_if_ordinary)),
    countIf(toString(tuple_array) != toString(tuple_array_ordinary)),
    countIf(toString(merge_map) != toString(merge_map_ordinary)),
    countIf(toString(merge_for_each) != toString(merge_for_each_ordinary))
FROM
(
    SELECT
        (round(r_tuple_if.1, 6) + 0, round(r_tuple_if.2, 6) + 0) AS tuple_if,
        (round(r_tuple_if_ordinary.1, 6) + 0, round(r_tuple_if_ordinary.2, 6) + 0) AS tuple_if_ordinary,
        (round(r_tuple_array.1, 6) + 0, round(r_tuple_array.2, 6) + 0) AS tuple_array,
        (round(r_tuple_array_ordinary.1, 6) + 0, round(r_tuple_array_ordinary.2, 6) + 0) AS tuple_array_ordinary,
        mapApply((key, value) -> (key, round(value, 6) + 0), r_merge_map) AS merge_map,
        mapApply((key, value) -> (key, round(value, 6) + 0), r_merge_map_ordinary) AS merge_map_ordinary,
        arrayMap(x -> round(x, 6) + 0, r_merge_for_each) AS merge_for_each,
        arrayMap(x -> round(x, 6) + 0, r_merge_for_each_ordinary) AS merge_for_each_ordinary
    FROM
    (
        SELECT
            contingencyTupleIfMerge(s_tuple_if) OVER w AS r_tuple_if,
            finalizeAggregation(contingencyTupleIfMergeState(s_tuple_if) OVER w) AS r_tuple_if_ordinary,
            contingencyTupleArrayMerge(s_tuple_array) OVER w AS r_tuple_array,
            finalizeAggregation(contingencyTupleArrayMergeState(s_tuple_array) OVER w) AS r_tuple_array_ordinary,
            contingencyMergeMapMerge(s_merge_map) OVER w AS r_merge_map,
            finalizeAggregation(contingencyMergeMapMergeState(s_merge_map) OVER w) AS r_merge_map_ordinary,
            contingencyMergeForEachMerge(s_merge_for_each) OVER w AS r_merge_for_each,
            finalizeAggregation(contingencyMergeForEachMergeState(s_merge_for_each) OVER w) AS r_merge_for_each_ordinary
        FROM
        (
            SELECT
                p,
                k,
                contingencyTupleIfState((b_any, c_any), (c_any, d_any), b_any != 1) OVER w AS s_tuple_if,
                contingencyTupleArrayState([(b_any, c_any)], [(c_any, d_any)]) OVER w AS s_tuple_array,
                contingencyMergeMapState(s0, k % 3) OVER w AS s_merge_map,
                contingencyMergeForEachState(if(k % 2 = 0, [s0], [s0, s1])) OVER w AS s_merge_for_each
            FROM
            (
                SELECT p, k, any(b) AS b_any, any(c) AS c_any, any(d) AS d_any, contingencyState(b, c) AS s0, contingencyState(c, d) AS s1
                FROM t_crosstab_window_wrapped
                GROUP BY p, k
            )
            WINDOW w AS (PARTITION BY p ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        )
        WINDOW w AS (PARTITION BY p ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    )
);

SELECT '-- whole partition: window merges of window states, then the plain aggregates';
SELECT DISTINCT p, round((contingencyMergeMapMerge(s) OVER (PARTITION BY p))[0], 6)
FROM
(
    SELECT p, contingencyMergeMapState(s0, 0) OVER (PARTITION BY p) AS s
    FROM (SELECT p, k, contingencyState(b, c) AS s0 FROM t_crosstab_window_wrapped GROUP BY p, k)
)
ORDER BY p;
SELECT DISTINCT p, round((contingencyTupleIfMerge(s) OVER (PARTITION BY p)).1, 6)
FROM (SELECT p, contingencyTupleIfState(tuple(b), tuple(c), 1) OVER (PARTITION BY p) AS s FROM t_crosstab_window_wrapped)
ORDER BY p;
SELECT p, round(contingency(b, c), 6) FROM t_crosstab_window_wrapped GROUP BY p ORDER BY p;

SELECT '-- a state with other parameters is still rejected';
SELECT contingencyResampleMerge(10, 30, 10)(s) OVER ()
FROM (SELECT contingencyResampleState(0, 20, 10)(b, c, k) AS s FROM t_crosstab_window_wrapped GROUP BY p); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE t_crosstab_window_wrapped;
