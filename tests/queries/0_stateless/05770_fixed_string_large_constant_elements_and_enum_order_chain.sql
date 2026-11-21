-- A large element of a constant array compared with an array of another type is not copied for every row:
-- a plain `FixedString` leaf, a `Dynamic` holding an array, and a nested array.
SELECT countIf([toString(number)] = [toFixedString('a', 1000000)]) FROM numbers(1000) SETTINGS max_block_size = 1000, max_memory_usage = 100000000, max_threads = 1;
SELECT countIf([toString(number)] < [toFixedString('a', 1000000)]) FROM numbers(1000) SETTINGS max_block_size = 1000, max_memory_usage = 100000000, max_threads = 1;
SELECT countIf(arrayMap(x -> toFixedString('a', 16), range(number % 2)) = [CAST(arrayResize(['a'], 10000) AS Dynamic)]) FROM numbers(10000) SETTINGS max_block_size = 10000, max_memory_usage = 100000000, max_threads = 1;
SELECT countIf(arrayMap(x -> toFixedString('a', 16), range(number % 2)) < [CAST(arrayResize(['a'], 10000) AS Dynamic)]) FROM numbers(10000) SETTINGS max_block_size = 10000, max_memory_usage = 100000000, max_threads = 1;
SELECT countIf([arrayMap(x -> toString(x), range(number % 2))] = [arrayResize([toFixedString('a', 16)], 10000)]) FROM numbers(10000) SETTINGS max_block_size = 10000, max_memory_usage = 100000000, max_threads = 1;

-- The constant and the materialized arrays give the same results.
WITH arrayMap(i -> toFixedString(toString(i % 7), 200), range(5)) AS c
SELECT sum(cityHash64(a = c, a < c, c <= a, isNotDistinctFrom(a, c))) = sum(cityHash64(a = materialize(c), a < materialize(c), materialize(c) <= a, isNotDistinctFrom(a, materialize(c))))
FROM (SELECT arrayMap(i -> toString((number + i) % 7), range(number % 7)) AS a FROM numbers(10000));
SELECT [toFixedString('ab', 100)] = [x], [toFixedString('ab', 100)] < [x], [x] > [toFixedString('ab', 100)], [x] = [toFixedString('ab', 100)]
FROM (SELECT arrayJoin(['ab', 'ab\0', 'ac', 'aa', '']) AS x);

-- An ordered comparison of an `Enum` with a `FixedString` constant is by name, so it does not join
-- the transitive chains of `optimize_and_compare_chain`, which compare the ordinals.
DROP TABLE IF EXISTS e;
CREATE TABLE e (k Enum8('a' = 2, 'b' = 1), j Enum8('a' = 2, 'b' = 1)) ENGINE = Memory;
INSERT INTO e VALUES ('b', 'b');
SELECT count() FROM e WHERE k > toFixedString('a', 2) AND k = j AND j = CAST('b' AS Enum8('a' = 2, 'b' = 1))
SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0, optimize_constant_columns_after_filter = 0;
SELECT count() FROM e WHERE k > toFixedString('a', 2) AND k = j AND j = CAST('b' AS Enum8('a' = 2, 'b' = 1))
SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1, optimize_constant_columns_after_filter = 0;
SELECT count() FROM e WHERE k > toFixedString('a', 2) AND k = j AND j = CAST('b' AS Enum8('a' = 2, 'b' = 1))
SETTINGS optimize_redundant_comparisons = 1, optimize_and_compare_chain = 1, optimize_constant_columns_after_filter = 1;
SELECT count() FROM e WHERE j < toFixedString('c', 2) AND k = j AND k = CAST('b' AS Enum8('a' = 2, 'b' = 1))
SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 1, optimize_constant_columns_after_filter = 0;
SELECT count() FROM e WHERE k = toFixedString('b', 2) AND k = j AND j = CAST('b' AS Enum8('a' = 2, 'b' = 1))
SETTINGS optimize_and_compare_chain = 1;
DROP TABLE e;
