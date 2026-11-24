-- A `FixedString` inside a composite constant is compared with a `String` zero-padded element by element, so
-- the logical expression optimizer must not treat the constant converted to the type of the column as the only
-- value it matches: no rewrite of the equality chain to `IN`, no fold of the conjunction to false.
SELECT '-- composite constants';
DROP TABLE IF EXISTS t_array;
DROP TABLE IF EXISTS t_tuple;
CREATE TABLE t_array (k Array(String)) ENGINE = Memory;
INSERT INTO t_array VALUES (['a']);
CREATE TABLE t_tuple (k Tuple(String)) ENGINE = Memory;
INSERT INTO t_tuple VALUES (tuple('a'));

SELECT count() FROM t_array
WHERE k = [toFixedString('a', 2)] OR k = [toFixedString('b', 2)] OR k = [toFixedString('c', 2)]
SETTINGS optimize_min_equality_disjunction_chain_length = 100, optimize_constant_columns_after_filter = 0;
SELECT count() FROM t_array
WHERE k = [toFixedString('a', 2)] OR k = [toFixedString('b', 2)] OR k = [toFixedString('c', 2)]
SETTINGS optimize_min_equality_disjunction_chain_length = 3, optimize_constant_columns_after_filter = 0;
SELECT count() FROM t_array WHERE k = [toFixedString('a', 2)] AND k = ['a'];
SELECT count() FROM t_array
WHERE k != [toFixedString('a', 2)] AND k != [toFixedString('b', 2)] AND k != [toFixedString('c', 2)]
SETTINGS optimize_min_equality_disjunction_chain_length = 3;

SELECT count() FROM t_tuple
WHERE k = tuple(toFixedString('a', 2)) OR k = tuple(toFixedString('b', 2)) OR k = tuple(toFixedString('c', 2))
SETTINGS optimize_min_equality_disjunction_chain_length = 100, optimize_constant_columns_after_filter = 0;
SELECT count() FROM t_tuple
WHERE k = tuple(toFixedString('a', 2)) OR k = tuple(toFixedString('b', 2)) OR k = tuple(toFixedString('c', 2))
SETTINGS optimize_min_equality_disjunction_chain_length = 3, optimize_constant_columns_after_filter = 0;
SELECT count() FROM t_tuple WHERE k = tuple(toFixedString('a', 2)) AND k = tuple('a');

DROP TABLE t_array;
DROP TABLE t_tuple;

-- Single-argument `concat` over `Dynamic` or `Variant` is a `toString` built from the overload resolver for each
-- alternative, so the hash of the function (the key of the query condition cache) includes the conversion mode
-- the resolver captured.
SELECT '-- concat in the query condition cache';
DROP TABLE IF EXISTS t_dynamic;
DROP TABLE IF EXISTS t_variant;
CREATE TABLE t_dynamic (id UInt64, k Dynamic) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dynamic SELECT number, toFixedString('a', 2) FROM numbers(20000);
CREATE TABLE t_variant (id UInt64, k Variant(FixedString(2), UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_variant SELECT number, toFixedString('a', 2) FROM numbers(20000);

SELECT count() FROM t_dynamic WHERE concat(k) = 'a'
SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 0;
SELECT count() FROM t_dynamic WHERE concat(k) = 'a'
SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 1;
SELECT count() FROM t_dynamic WHERE concat(k) = 'a'
SETTINGS use_query_condition_cache = 0, cast_fixed_string_to_string_strip_trailing_zeros = 1;

SELECT count() FROM t_variant WHERE concat(k) = 'a'
SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 0;
SELECT count() FROM t_variant WHERE concat(k) = 'a'
SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 1;
SELECT count() FROM t_variant WHERE concat(k) = 'a'
SETTINGS use_query_condition_cache = 0, cast_fixed_string_to_string_strip_trailing_zeros = 1;

DROP TABLE t_dynamic;
DROP TABLE t_variant;

-- The null-safe comparison of a `Nullable` value with a `Nullable` constant `FixedString` keeps the constant:
-- a large constant is not replicated for every row.
SELECT '-- large constants in the null-safe comparison';
SELECT
    countIf(isNotDistinctFrom(
        CAST(if(number % 2, 'a', 'b') AS Nullable(Enum8('a' = 1, 'b' = 2))),
        toNullable(toFixedString('a', 1000000)))),
    countIf(isDistinctFrom(
        CAST(if(number % 2, 'a', 'b') AS Nullable(Enum8('a' = 1, 'b' = 2))),
        toNullable(toFixedString('a', 1000000))))
FROM numbers(1000)
SETTINGS max_block_size = 1000, max_memory_usage = 100000000, max_threads = 1;

SELECT
    countIf(isNotDistinctFrom(
        tuple(if(number % 2, 'a', 'b'), toNullable(number % 3)),
        toNullable(tuple(toFixedString('a', 1000000), 1)))),
    countIf(isDistinctFrom(
        if(number % 5 = 0, NULL, tuple(if(number % 2, 'a', 'b'), number % 3)),
        toNullable(tuple(toFixedString('a', 1000000), 1))))
FROM numbers(1000)
SETTINGS max_block_size = 1000, max_memory_usage = 100000000, max_threads = 1;

-- The same values without constants.
SELECT
    countIf(isNotDistinctFrom(
        tuple(if(number % 2, 'a', 'b'), toNullable(number % 3)),
        materialize(toNullable(tuple(toFixedString('a', 4), 1))))),
    countIf(isDistinctFrom(
        if(number % 5 = 0, NULL, tuple(if(number % 2, 'a', 'b'), number % 3)),
        materialize(toNullable(tuple(toFixedString('a', 4), 1)))))
FROM numbers(1000);

-- NULL constants.
SELECT
    countIf(isNotDistinctFrom(
        CAST(if(number % 3, 'a', NULL) AS Nullable(Enum8('a' = 1, 'b' = 2))),
        CAST(NULL AS Nullable(FixedString(3))))),
    countIf(isNotDistinctFrom(
        if(number % 5 = 0, NULL, tuple(if(number % 2, 'a', 'b'), number % 3)),
        CAST(NULL AS Nullable(Tuple(FixedString(3), UInt8))))),
    isNotDistinctFrom(toNullable(tuple('a', 1)), toNullable(tuple(toFixedString('a', 3), 1))),
    isNotDistinctFrom(toNullable('a'), toNullable(toFixedString('a', 3))),
    isNotDistinctFrom(CAST(NULL AS Nullable(String)), toNullable(toFixedString('a', 3))),
    isNotDistinctFrom(CAST(NULL AS Nullable(String)), CAST(NULL AS Nullable(FixedString(3))))
FROM numbers(1000);
