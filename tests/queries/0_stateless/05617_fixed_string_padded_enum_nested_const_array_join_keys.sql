-- A `FixedString` is compared with an `Enum` by its value without the trailing zero bytes. Index analysis and the
-- rewrites of comparisons follow it also for a padded constant, and for an `Enum` name that ends with a zero byte.

SELECT 'padded constant, tuple key';
DROP TABLE IF EXISTS t_enum_tuple_padded;
CREATE TABLE t_enum_tuple_padded (a Tuple(Enum8('z' = 0, 'a' = 1, 'a\0' = 2)))
ENGINE = MergeTree PARTITION BY cityHash64(a) ORDER BY tuple();
INSERT INTO t_enum_tuple_padded VALUES (tuple('a')), (tuple('a\0'));
SELECT hex(toString(a.1)) FROM t_enum_tuple_padded WHERE a = tuple(toFixedString('a', 2))
SETTINGS use_partition_pruning = 0, optimize_use_implicit_projections = 0;
SELECT hex(toString(a.1)) FROM t_enum_tuple_padded WHERE a = tuple(toFixedString('a', 2))
SETTINGS use_partition_pruning = 1, optimize_use_implicit_projections = 0;
DROP TABLE t_enum_tuple_padded;

SELECT 'padded constant, rewrites';
DROP TABLE IF EXISTS t_enum_chain_padded;
CREATE TABLE t_enum_chain_padded (e Enum8('z' = 0, 'a' = 1, 'a\0' = 2, 'b' = 3, 'c' = 4)) ENGINE = Memory;
INSERT INTO t_enum_chain_padded VALUES ('a'), ('a\0'), ('b'), ('z');
SELECT arraySort(groupArray(hex(toString(e)))) FROM t_enum_chain_padded
WHERE e = toFixedString('a', 2) OR e = toFixedString('b', 2) OR e = toFixedString('c', 2)
SETTINGS optimize_min_equality_disjunction_chain_length = 3, optimize_constant_columns_after_filter = 0;
SELECT arraySort(groupArray(hex(toString(e)))) FROM t_enum_chain_padded
WHERE e = toFixedString('a', 2) OR e = toFixedString('b', 2) OR e = toFixedString('c', 2)
SETTINGS optimize_min_equality_disjunction_chain_length = 100, optimize_constant_columns_after_filter = 0;
SELECT arraySort(groupArray(hex(toString(e)))) FROM t_enum_chain_padded
WHERE e = toFixedString('a', 2) AND e = CAST('a', 'Enum8(\'z\' = 0, \'a\' = 1, \'a\\0\' = 2, \'b\' = 3, \'c\' = 4)')
SETTINGS optimize_constant_columns_after_filter = 0;
SELECT arraySort(groupArray(hex(toString(e)))) FROM t_enum_chain_padded
WHERE e != toFixedString('a', 2) AND e != toFixedString('b', 2) AND e != toFixedString('c', 2)
SETTINGS optimize_min_equality_disjunction_chain_length = 3, optimize_constant_columns_after_filter = 0;
DROP TABLE t_enum_chain_padded;

SELECT 'enum name with a trailing zero byte against a FixedString key';
DROP TABLE IF EXISTS t_fixed_string_key;
CREATE TABLE t_fixed_string_key (k FixedString(2)) ENGINE = MergeTree PARTITION BY k ORDER BY k;
INSERT INTO t_fixed_string_key VALUES ('a');
INSERT INTO t_fixed_string_key VALUES ('b');
SELECT hex(k) FROM t_fixed_string_key WHERE k < CAST('a\0', 'Enum8(\'a\\0\' = 1)') ORDER BY k
SETTINGS use_partition_pruning = 0, use_primary_key = 0, optimize_use_implicit_projections = 0;
SELECT hex(k) FROM t_fixed_string_key WHERE k < CAST('a\0', 'Enum8(\'a\\0\' = 1)') ORDER BY k
SETTINGS use_partition_pruning = 1, use_primary_key = 1, optimize_use_implicit_projections = 0;
SELECT hex(k) FROM t_fixed_string_key WHERE k != CAST('a\0', 'Enum8(\'a\\0\' = 1)') ORDER BY k
SETTINGS use_partition_pruning = 0, use_primary_key = 0, optimize_use_implicit_projections = 0;
SELECT hex(k) FROM t_fixed_string_key WHERE k != CAST('a\0', 'Enum8(\'a\\0\' = 1)') ORDER BY k
SETTINGS use_partition_pruning = 1, use_primary_key = 1, optimize_use_implicit_projections = 0;
SELECT hex(k) FROM t_fixed_string_key WHERE k = CAST('a', 'Enum8(\'a\' = 1)') ORDER BY k
SETTINGS use_partition_pruning = 1, use_primary_key = 1, optimize_use_implicit_projections = 0;
DROP TABLE t_fixed_string_key;

-- A constant array with arrays inside is not copied for every row.
SELECT 'nested constant array';
SELECT countIf([arrayMap(x -> toString(x), range(number % 2))] = [arrayResize([toFixedString('a', 16)], 10000)])
FROM numbers(10000)
SETTINGS max_block_size = 10000, max_memory_usage = 100000000, max_threads = 1;
SELECT countIf([arrayResize([toFixedString('a', 16)], 10000)] < [arrayMap(x -> toString(x), range(number % 2))])
FROM numbers(10000)
SETTINGS max_block_size = 10000, max_memory_usage = 100000000, max_threads = 1;
SELECT countIf([(1, arrayMap(x -> toString(x), range(number % 2)))] = [(1, arrayResize([toFixedString('a', 16)], 10000))])
FROM numbers(10000)
SETTINGS max_block_size = 10000, max_memory_usage = 100000000, max_threads = 1;
SELECT groupArray([arrayMap(x -> toString(x), range(number % 3)), ['5']] = [[toFixedString('0', 2), toFixedString('1', 2)], [toFixedString('5', 2)]]) FROM numbers(6);
SELECT groupArray([arrayMap(x -> toString(x), range(number % 3)), ['5']] = materialize([[toFixedString('0', 2), toFixedString('1', 2)], [toFixedString('5', 2)]])) FROM numbers(6);
SELECT groupArray([[toFixedString('0', 2)]] < [arrayMap(x -> toString(x), range(number % 3))]) FROM numbers(6);
SELECT groupArray(materialize([[toFixedString('0', 2)]]) < [arrayMap(x -> toString(x), range(number % 3))]) FROM numbers(6);

-- The join keys convert a `FixedString` to `String` without its trailing zero bytes, which is not the comparison of
-- arrays with `FixedString` elements, so such a comparison is not a join key.
SELECT 'array join keys';
DROP TABLE IF EXISTS t_array_fixed_string;
DROP TABLE IF EXISTS t_array_string;
CREATE TABLE t_array_fixed_string (k Array(FixedString(2))) ENGINE = Memory;
CREATE TABLE t_array_string (k Array(String)) ENGINE = Memory;
INSERT INTO t_array_fixed_string VALUES (['a']), (['b']);
INSERT INTO t_array_string VALUES (['a\0']), (['a']), (['c']);
SELECT count() FROM t_array_fixed_string AS f CROSS JOIN t_array_string AS s WHERE f.k = s.k
SETTINGS cross_to_inner_join_rewrite = 0, query_plan_merge_filter_into_join_condition = 0, optimize_constant_columns_after_filter = 0;
SELECT count() FROM t_array_fixed_string AS f CROSS JOIN t_array_string AS s WHERE f.k = s.k
SETTINGS cross_to_inner_join_rewrite = 1, query_plan_merge_filter_into_join_condition = 0, optimize_constant_columns_after_filter = 0;
SELECT count() FROM t_array_fixed_string AS f CROSS JOIN t_array_string AS s WHERE f.k = s.k
SETTINGS cross_to_inner_join_rewrite = 1, query_plan_merge_filter_into_join_condition = 1;
SELECT count() FROM t_array_fixed_string AS f JOIN t_array_string AS s ON f.k = s.k;
SELECT hex(toString(f.k[1])), arraySort(groupArray(hex(s.k[1]))) FROM t_array_fixed_string AS f LEFT JOIN t_array_string AS s ON f.k = s.k
GROUP BY f.k ORDER BY f.k;
DROP TABLE t_array_fixed_string;
DROP TABLE t_array_string;
