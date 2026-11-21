-- A `FixedString` is compared with a `String` zero-padded, while its conversion to `String` keeps the padding.

-- Arrays of tuples with `Nullable` elements, compared element by element. A NULL is equal only to another NULL
-- and sorts after every non-NULL value.
SELECT [tuple(toNullable(toFixedString('a', 2)))] < [tuple(toNullable('b'))];
SELECT [tuple(toNullable(toFixedString('b', 2)))] < [tuple(toNullable('a'))];
SELECT [tuple(toNullable(toFixedString('a', 2)))] < [tuple(CAST(NULL, 'Nullable(String)'))];
SELECT [tuple(CAST(NULL, 'Nullable(FixedString(2))'))] < [tuple(toNullable('a'))];
SELECT [tuple(CAST(NULL, 'Nullable(FixedString(2))'))] <= [tuple(CAST(NULL, 'Nullable(String)'))];
SELECT [tuple(toNullable(toFixedString('a', 2)), 1)] < [tuple(toNullable('a'), 2)];
SELECT [tuple(toNullable(toFixedString('a', 2)), 2)] > [tuple(toNullable('a'), 1)];
SELECT [tuple(toNullable(toFixedString('a', 2)), 1)] >= [tuple(toNullable('a'), 2)];
SELECT [tuple(toNullable(toFixedString('a', 2)))] = [tuple(toNullable('a'))];

-- Null-safe comparisons of `Nullable` arguments.
SELECT isNotDistinctFrom(toFixedString('a', 2), 'a');
SELECT isNotDistinctFrom(toNullable(toFixedString('a', 2)), toNullable('a'));
SELECT isDistinctFrom(toNullable(toFixedString('a', 2)), toNullable('a'));
SELECT isNotDistinctFrom(materialize(toNullable(toFixedString('a', 2))), materialize(toNullable('a')));
SELECT isNotDistinctFrom(toLowCardinality(toNullable(toFixedString('a', 2))), toNullable('a'));
SELECT isNotDistinctFrom(toNullable(toFixedString('a', 2)), toNullable('b'));
SELECT isNotDistinctFrom(CAST(NULL, 'Nullable(FixedString(2))'), toNullable('a'));
SELECT isNotDistinctFrom(CAST(NULL, 'Nullable(FixedString(2))'), CAST(NULL, 'Nullable(String)'));

-- The rewrite of an `if` chain to `multiIf` is skipped with `cast_fixed_string_to_string_strip_trailing_zeros`,
-- so `if` keeps the padding and `multiIf` removes it.
SELECT hex(if(number = 0, 'x', if(number = 1, toFixedString('a', 2), 'y'))) AS a, hex(multiIf(number = 0, 'x', number = 1, toFixedString('a', 2), 'y')) AS b
FROM numbers(2)
SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1, optimize_if_chain_to_multiif = 1;

-- The query condition cache distinguishes `coalesce` and `caseWithExpression` by the setting.
DROP TABLE IF EXISTS t_fixed_string_wrapper_hash;
CREATE TABLE t_fixed_string_wrapper_hash (id UInt64, f Nullable(FixedString(2)), g Nullable(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_fixed_string_wrapper_hash SELECT number, 'a', NULL FROM numbers(20000);

SELECT count() FROM t_fixed_string_wrapper_hash WHERE coalesce(f, g, 'x') = 'a'
SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 0;
SELECT count() FROM t_fixed_string_wrapper_hash WHERE coalesce(f, g, 'x') = 'a'
SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 1;

SELECT count() FROM t_fixed_string_wrapper_hash WHERE caseWithExpression(id % 2, 0, f, 1, g, 'x') = 'a'
SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 0;
SELECT count() FROM t_fixed_string_wrapper_hash WHERE caseWithExpression(id % 2, 0, f, 1, g, 'x') = 'a'
SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 1;

DROP TABLE t_fixed_string_wrapper_hash;
