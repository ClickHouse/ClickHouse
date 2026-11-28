-- The functions that run an aggregate (`arrayReduce`, `arrayReduceInRanges`, `initializeAggregation`) take the
-- conversion mode captured by `groupConcat` into their hash, so the query condition cache tells the modes apart.

DROP TABLE IF EXISTS t_reduce;
CREATE TABLE t_reduce (id UInt64, a Array(FixedString(2))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_reduce SELECT number, [toFixedString('a', 2)] FROM numbers(20000);

SELECT 'arrayReduce';
SELECT count() FROM t_reduce WHERE arrayReduce('groupConcat', a) = 'a' SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 0;
SELECT count() FROM t_reduce WHERE arrayReduce('groupConcat', a) = 'a' SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 1;
SELECT count() FROM t_reduce WHERE arrayReduce('groupConcat', a) = 'a' SETTINGS use_query_condition_cache = 0, cast_fixed_string_to_string_strip_trailing_zeros = 1;

SELECT 'arrayReduceInRanges';
SELECT count() FROM t_reduce WHERE arrayReduceInRanges('groupConcat', [(1, 1)], a)[1] = 'a' SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 0;
SELECT count() FROM t_reduce WHERE arrayReduceInRanges('groupConcat', [(1, 1)], a)[1] = 'a' SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 1;

SELECT 'initializeAggregation';
SELECT count() FROM t_reduce WHERE finalizeAggregation(initializeAggregation('groupConcatState', a[1])) = 'a' SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 0;
SELECT count() FROM t_reduce WHERE finalizeAggregation(initializeAggregation('groupConcatState', a[1])) = 'a' SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 1;

SELECT 'combinator';
SELECT count() FROM t_reduce WHERE arrayReduce('groupConcatIf', a, [1]) = 'a' SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 0;
SELECT count() FROM t_reduce WHERE arrayReduce('groupConcatIf', a, [1]) = 'a' SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 1;

DROP TABLE t_reduce;

-- An `Enum` is compared with a constant `FixedString` as with a full column: by its name, without the trailing zero bytes of the `FixedString`.
SELECT 'enum';
SELECT
    materialize(CAST('a\0' AS Enum8('a' = 1, 'a\0' = 2))) = toFixedString('a', 1),
    materialize(CAST('a\0' AS Enum8('a' = 1, 'a\0' = 2))) = materialize(toFixedString('a', 1));
SELECT
    materialize(CAST('a' AS Enum8('a' = 1, 'a\0' = 2))) = toFixedString('a', 2),
    materialize(CAST('a' AS Enum8('a' = 1, 'a\0' = 2))) = materialize(toFixedString('a', 2));
SELECT
    toFixedString('b', 1) < materialize(CAST('a' AS Enum8('a' = 1, 'b' = 2))),
    materialize(toFixedString('b', 1)) < materialize(CAST('a' AS Enum8('a' = 1, 'b' = 2)));
SELECT
    materialize(CAST('a' AS Enum8('a' = 1))) != toFixedString('x', 3),
    materialize(CAST('a' AS Enum8('a' = 1))) != materialize(toFixedString('x', 3));
