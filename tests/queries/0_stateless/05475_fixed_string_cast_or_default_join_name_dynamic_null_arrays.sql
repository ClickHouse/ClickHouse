-- `accurateCastOrDefault` keeps the conversion mode of its scope in a serialized plan.
SELECT count()
FROM
(
    SELECT accurateCastOrDefault(toFixedString(toString(number), 2), 'String') AS v
    FROM numbers(1)
    SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1
)
WHERE v = '0'
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, cast_fixed_string_to_string_strip_trailing_zeros = 0;

SELECT count()
FROM
(
    SELECT accurateCastOrDefault(toFixedString(toString(number), 2), 'String') AS v
    FROM numbers(1)
    SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1
)
WHERE v = '0'
SETTINGS make_distributed_plan = 0, cast_fixed_string_to_string_strip_trailing_zeros = 0;

-- A `JOIN` with a key converted from `FixedString` internally and the same `_CAST` written in the query.
DROP TABLE IF EXISTS f;
DROP TABLE IF EXISTS s;
CREATE TABLE f (fs FixedString(2)) ENGINE = Memory;
CREATE TABLE s (a String, b String) ENGINE = Memory;
INSERT INTO f VALUES ('a');
INSERT INTO s VALUES ('a', 'a\0');

SELECT count() FROM f JOIN s
ON f.fs = s.a AND _CAST(f.fs, 'String') = s.b
SETTINGS enable_join_runtime_filters = 0, query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0,
    cast_fixed_string_to_string_strip_trailing_zeros = 0;

SELECT count() FROM f JOIN s
ON f.fs = s.a AND _CAST(f.fs, 'String') = s.b
SETTINGS enable_join_runtime_filters = 1, query_plan_join_swap_table = 'true', cast_fixed_string_to_string_strip_trailing_zeros = 0;

DROP TABLE f;
DROP TABLE s;

-- A NULL of `Nullable(FixedString)` against a NULL of `Dynamic` in arrays.
SELECT
    [CAST(NULL AS Nullable(FixedString(2)))] = [CAST(NULL AS Dynamic)],
    [CAST(NULL AS Nullable(FixedString(2)))] != [CAST(NULL AS Dynamic)],
    [CAST(NULL AS Nullable(FixedString(2)))] <= [CAST(NULL AS Dynamic)],
    [CAST(NULL AS Nullable(FixedString(2)))] > [CAST(NULL AS Dynamic)];

SELECT
    [CAST(NULL AS Dynamic)] = [CAST(NULL AS Nullable(FixedString(2)))],
    [CAST(NULL AS Dynamic)] >= [CAST(NULL AS Nullable(FixedString(2)))],
    [CAST(NULL AS Dynamic)] < [CAST(NULL AS Nullable(FixedString(2)))];

-- A NULL sorts after a non-NULL value on either side.
SELECT
    [toNullable(toFixedString('a', 2))] < [CAST(NULL AS Dynamic)],
    [CAST(NULL AS Nullable(FixedString(2)))] < [CAST('a' AS Dynamic)],
    [CAST(NULL AS Nullable(FixedString(2))), toFixedString('a', 2)] = [CAST(NULL AS Dynamic), toFixedString('a', 2)::Dynamic];
