-- `CASE` keys, plan serialization, index analysis and array comparison with a `FixedString` carried by `Dynamic`,
-- or with a conversion mode set in a subquery.

SELECT 'CASE with Dynamic in a tuple key';
SELECT
    tuple('a') = tuple(CAST(toFixedString('a', 2) AS Dynamic)),
    CASE tuple('a') WHEN tuple(CAST(toFixedString('a', 2) AS Dynamic)) THEN 1 ELSE 0 END,
    CASE tuple('a') WHEN materialize(tuple(CAST(toFixedString('a', 2) AS Dynamic))) THEN 1 ELSE 0 END;

SELECT 'toString over Dynamic in a serialized plan';
SELECT count()
FROM
(
    SELECT toString(CAST(toFixedString(toString(number), 2) AS Dynamic)) AS v
    FROM numbers(1)
    SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1
)
WHERE v = '0'
SETTINGS make_distributed_plan = 0;
SELECT count()
FROM
(
    SELECT toString(CAST(toFixedString(toString(number), 2) AS Dynamic)) AS v
    FROM numbers(1)
    SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1
)
WHERE v = '0'
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

SELECT 'toString in key analysis with a mode from another scope';
DROP TABLE IF EXISTS t_fs_key_mode;
CREATE TABLE t_fs_key_mode (k FixedString(2)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_fs_key_mode SELECT 'a' FROM numbers(1000);
INSERT INTO t_fs_key_mode VALUES ('b');
SELECT count()
FROM (SELECT k FROM t_fs_key_mode SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 0)
WHERE toString(k) = 'a'
SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1, use_primary_key = 0;
SELECT count()
FROM (SELECT k FROM t_fs_key_mode SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 0)
WHERE toString(k) = 'a'
SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1;
DROP TABLE t_fs_key_mode;

SELECT 'Array of FixedString against array of Dynamic';
SELECT
    [toFixedString('a', 2)] =  [CAST('a' AS Dynamic)],
    [toFixedString('a', 2)] <  [CAST('a' AS Dynamic)],
    [toFixedString('a', 2)] <= [CAST('a' AS Dynamic)],
    [toFixedString('a', 2)] >= [CAST('a' AS Dynamic)],
    [toFixedString('a', 2)] >  [CAST('a' AS Dynamic)];
