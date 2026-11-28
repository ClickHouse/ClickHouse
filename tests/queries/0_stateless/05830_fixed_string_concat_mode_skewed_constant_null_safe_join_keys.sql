-- The conversion mode of a `FixedString` converted to `String` by single-argument `concat` over `Dynamic` survives the
-- serialization of the query plan: `concat` delegates to a `toString` created from the scope's context.
SELECT count()
FROM
(
    SELECT concat(CAST(toFixedString(toString(number), 2) AS Dynamic)) AS v
    FROM numbers(1)
    SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1
)
WHERE v = '0'
SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 0, make_distributed_plan = 1, distributed_plan_execute_locally = 1;

SELECT count()
FROM
(
    SELECT concat(CAST(toFixedString(toString(number), 2) AS Dynamic)) AS v
    FROM numbers(1)
    SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1
)
WHERE v = '0'
SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 0, make_distributed_plan = 0;

SELECT count()
FROM
(
    SELECT concat(CAST(toFixedString(toString(number), 2) AS Dynamic)) AS v
    FROM numbers(1)
    SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 0
)
WHERE v = '0\0'
SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1;

-- A constant array with one large element among many small ones is not copied for every row when compared element
-- by element with an array of another element type.
SELECT countIf(
    [toFixedString(toString(number % 10), 1)]
    = arrayConcat([repeat('a', 1000000)], arrayResize([''], 20000)))
FROM numbers(1000)
SETTINGS max_block_size = 1000, max_memory_usage = 100000000, max_threads = 1;

SELECT countIf(
    [toFixedString(toString(number % 10), 1)]
    < arrayConcat([repeat('a', 1000000)], arrayResize([''], 20000)))
FROM numbers(1000)
SETTINGS max_block_size = 1000, max_memory_usage = 100000000, max_threads = 1;

-- The same constant and its materialized copy give the same results.
SELECT
    countIf([toFixedString(toString(number % 3), 1), 'x'] = arrayConcat([repeat('a', 1000)], ['x'], arrayResize([''], 2000))),
    countIf([toFixedString(toString(number % 3), 1), 'x'] = materialize(arrayConcat([repeat('a', 1000)], ['x'], arrayResize([''], 2000)))),
    countIf([toFixedString('a', 1)] < arrayConcat(['a', repeat('b', 1000)], arrayResize([''], 2000))),
    countIf([toFixedString('a', 1)] < materialize(arrayConcat(['a', repeat('b', 1000)], arrayResize([''], 2000))))
FROM numbers(10);

-- A null-safe comparison of a `Nullable(FixedString)` with a `Nullable(String)`, also inside a tuple, matches the same
-- rows as a predicate over the joined rows and as a join key.
DROP TABLE IF EXISTS f;
DROP TABLE IF EXISTS s;
DROP TABLE IF EXISTS ft;
DROP TABLE IF EXISTS st;
CREATE TABLE f (k Nullable(FixedString(2))) ENGINE = Memory;
CREATE TABLE s (k Nullable(String)) ENGINE = Memory;
CREATE TABLE ft (k Nullable(Tuple(FixedString(2)))) ENGINE = Memory;
CREATE TABLE st (k Nullable(Tuple(String))) ENGINE = Memory;
INSERT INTO f VALUES ('a'), (NULL);
INSERT INTO s SELECT arrayJoin([unhex('6100'), 'a', 'a\0\0', NULL]);
INSERT INTO ft VALUES (tuple('a')), (NULL);
INSERT INTO st SELECT arrayJoin([tuple(unhex('6100')), tuple('a'), NULL]);

SELECT count() FROM f CROSS JOIN s WHERE isNotDistinctFrom(f.k, s.k)
SETTINGS cross_to_inner_join_rewrite = 0, query_plan_merge_filter_into_join_condition = 0;
SELECT count() FROM f JOIN s ON isNotDistinctFrom(f.k, s.k) SETTINGS enable_join_runtime_filters = 0;
SELECT count() FROM f JOIN s ON isNotDistinctFrom(f.k, s.k) SETTINGS enable_join_runtime_filters = 1;

SELECT count() FROM ft CROSS JOIN st WHERE isNotDistinctFrom(ft.k, st.k)
SETTINGS cross_to_inner_join_rewrite = 0, query_plan_merge_filter_into_join_condition = 0;
SELECT count() FROM ft JOIN st ON isNotDistinctFrom(ft.k, st.k) SETTINGS enable_join_runtime_filters = 0;
SELECT count() FROM ft JOIN st ON isNotDistinctFrom(ft.k, st.k) SETTINGS enable_join_runtime_filters = 1;

DROP TABLE f;
DROP TABLE s;
DROP TABLE ft;
DROP TABLE st;
