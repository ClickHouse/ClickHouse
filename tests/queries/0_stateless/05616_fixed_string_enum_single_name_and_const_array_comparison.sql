-- An `Enum` is compared with a `FixedString` by its name, with the trailing zero bytes of the `FixedString` removed,
-- so a `FixedString` value equals at most one name. A join, a rewrite of an equality chain and index analysis over a
-- composite key give the same result as the plain comparison.

SELECT toFixedString('a', 1) = CAST('a', 'Enum8(\'a\' = 1, \'a\\0\' = 2)'), toFixedString('a', 1) = CAST('a\0', 'Enum8(\'a\' = 1, \'a\\0\' = 2)');
SELECT toFixedString('a', 2) = CAST('a', 'Enum8(\'a\' = 1, \'a\\0\' = 2)'), toFixedString('a', 2) = CAST('a\0', 'Enum8(\'a\' = 1, \'a\\0\' = 2)');
SELECT materialize(toFixedString('a', 2)) = CAST('a', 'Enum8(\'a\' = 1, \'a\\0\' = 2)'), materialize(toFixedString('a', 2)) = materialize(CAST('a\0', 'Enum8(\'a\' = 1, \'a\\0\' = 2)'));

SELECT 'join';
DROP TABLE IF EXISTS t_enum_join_e;
DROP TABLE IF EXISTS t_enum_join_f;
CREATE TABLE t_enum_join_e (k Enum8('a' = 1, 'a\0' = 2)) ENGINE = Memory;
CREATE TABLE t_enum_join_f (k FixedString(1)) ENGINE = Memory;
INSERT INTO t_enum_join_e VALUES ('a'), ('a\0');
INSERT INTO t_enum_join_f VALUES ('a');
SELECT count() FROM t_enum_join_e AS e CROSS JOIN t_enum_join_f AS f WHERE e.k = f.k
SETTINGS cross_to_inner_join_rewrite = 0, query_plan_merge_filter_into_join_condition = 0, optimize_constant_columns_after_filter = 0;
SELECT count() FROM t_enum_join_e AS e CROSS JOIN t_enum_join_f AS f WHERE e.k = f.k
SETTINGS cross_to_inner_join_rewrite = 1, query_plan_merge_filter_into_join_condition = 0, optimize_constant_columns_after_filter = 0;
SELECT count() FROM t_enum_join_e AS e JOIN t_enum_join_f AS f ON e.k = f.k;
DROP TABLE t_enum_join_e;
DROP TABLE t_enum_join_f;

SELECT 'equality chain';
DROP TABLE IF EXISTS t_enum_chain;
CREATE TABLE t_enum_chain (e Enum8('z' = 0, 'a' = 1, 'a\0' = 2, 'b' = 3, 'c' = 4)) ENGINE = Memory;
INSERT INTO t_enum_chain VALUES ('a'), ('a\0'), ('b'), ('z');
SELECT arraySort(groupArray(hex(toString(e)))) FROM t_enum_chain
WHERE e = toFixedString('a', 1) OR e = toFixedString('b', 1) OR e = toFixedString('c', 1)
SETTINGS optimize_min_equality_disjunction_chain_length = 3, optimize_constant_columns_after_filter = 0;
SELECT arraySort(groupArray(hex(toString(e)))) FROM t_enum_chain
WHERE e = toFixedString('a', 1) OR e = toFixedString('b', 1) OR e = toFixedString('c', 1)
SETTINGS optimize_min_equality_disjunction_chain_length = 100, optimize_constant_columns_after_filter = 0;
SELECT count() FROM t_enum_chain WHERE e = toFixedString('a', 1) AND e = CAST('a\0', 'Enum8(\'z\' = 0, \'a\' = 1, \'a\\0\' = 2, \'b\' = 3, \'c\' = 4)');
DROP TABLE t_enum_chain;

SELECT 'tuple key';
DROP TABLE IF EXISTS t_enum_tuple;
CREATE TABLE t_enum_tuple (a Tuple(Enum8('z' = 0, 'a' = 1, 'a\0' = 2)))
ENGINE = MergeTree PARTITION BY cityHash64(a) ORDER BY tuple();
INSERT INTO t_enum_tuple VALUES (tuple('a')), (tuple('a\0'));
SELECT count() FROM t_enum_tuple WHERE a = tuple(toFixedString('a', 1))
SETTINGS use_partition_pruning = 0, optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0;
SELECT count() FROM t_enum_tuple WHERE a = tuple(toFixedString('a', 1))
SETTINGS use_partition_pruning = 1, optimize_use_implicit_projections = 0, optimize_trivial_count_query = 0;
DROP TABLE t_enum_tuple;

-- A constant array compared elementwise is not replicated for every row.
SELECT 'constant array';
SELECT countIf(arrayMap(x -> toString(x), range(number % 2)) = arrayResize([toFixedString('a', 16)], 10000))
FROM numbers(10000)
SETTINGS max_block_size = 10000, max_memory_usage = 100000000, max_threads = 1;
SELECT countIf(arrayResize([toFixedString('a', 16)], 10000) < arrayMap(x -> toString(x), range(number % 2)))
FROM numbers(10000)
SETTINGS max_block_size = 10000, max_memory_usage = 100000000, max_threads = 1;
SELECT countIf(arrayMap(x -> toString(x), range(number % 3)) = [toFixedString('0', 2), toFixedString('1', 2)]) FROM numbers(10);
SELECT countIf(arrayMap(x -> toString(x), range(number % 3)) = materialize([toFixedString('0', 2), toFixedString('1', 2)])) FROM numbers(10);
SELECT countIf([toFixedString('0', 2)] < arrayMap(x -> toString(x), range(number % 3))) FROM numbers(10);
SELECT countIf(materialize([toFixedString('0', 2)]) < arrayMap(x -> toString(x), range(number % 3))) FROM numbers(10);

-- The function running an aggregate over a `Dynamic` argument is hashed with the conversion mode its aggregate is
-- created with, which follows the query, not a subquery that overrides the setting.
SELECT 'scopes';
DROP TABLE IF EXISTS t_dyn_scope;
CREATE TABLE t_dyn_scope (id UInt64, a Dynamic) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dyn_scope SELECT number, [toFixedString('a', 2)] FROM numbers(1000);
SELECT count() FROM
(
    SELECT id FROM t_dyn_scope WHERE arrayReduceInRanges('groupConcat', [(1, 1)], a)[1] = 'a'
    SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 0, use_query_condition_cache = 1
)
SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 0;
SELECT count() FROM
(
    SELECT id FROM t_dyn_scope WHERE arrayReduceInRanges('groupConcat', [(1, 1)], a)[1] = 'a'
    SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 0, use_query_condition_cache = 1
)
SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1;
DROP TABLE t_dyn_scope;
