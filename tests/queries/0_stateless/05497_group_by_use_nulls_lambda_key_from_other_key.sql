-- A GROUP BY key with a lambda whose argument comes from another key must be read from the key column
-- in the rolled-up rows, as for keys without lambdas.

SET group_by_use_nulls = 1;

SELECT '-- ROLLUP';
SELECT number, arrayCount(x -> x > 0, [number]) AS k, count() FROM numbers(3) GROUP BY ROLLUP(number, k) ORDER BY ALL;

SELECT '-- CUBE';
SELECT number, arrayCount(x -> x > 0, [number]) AS k, count() FROM numbers(2) GROUP BY CUBE(number, k) ORDER BY ALL;

SELECT '-- GROUPING SETS';
SELECT number, arrayCount(x -> x > 0, [number]) AS k, count() FROM numbers(2) GROUP BY GROUPING SETS ((number), (k), ()) ORDER BY ALL;

SELECT '-- key repeated without an alias';
SELECT number, arrayCount(x -> x > 0, [number]), count() FROM numbers(2) GROUP BY ROLLUP(number, arrayCount(x -> x > 0, [number])) ORDER BY ALL;

SELECT '-- Array key';
SELECT number, arrayMap(x -> x + 1, [number]) AS k, toTypeName(k), count() FROM numbers(2) GROUP BY ROLLUP(number, k) ORDER BY ALL;

SELECT '-- arrayFold';
SELECT number, arrayFold((acc, x) -> acc + x, [number], toNullable(0::UInt64)) AS k, count() FROM numbers(2) GROUP BY ROLLUP(number, k) ORDER BY ALL;

SELECT '-- mapApply';
SELECT number, mapApply((key, value) -> (key, value + 1), map(1, number)) AS k, count() FROM numbers(2) GROUP BY ROLLUP(number, k) ORDER BY ALL;

SELECT '-- nested lambda';
SELECT number, arrayMap(x -> arrayCount(y -> y > x, [x, 1]), [number]) AS k, count() FROM numbers(2) GROUP BY ROLLUP(number, k) ORDER BY ALL;

SELECT '-- lambda under another function';
SELECT number, toString(arrayMap(x -> x + 1, [number])) AS k, count() FROM numbers(2) GROUP BY ROLLUP(number, k) ORDER BY ALL;

SELECT '-- key containing the lambda key';
SELECT number, k + 1, count() FROM numbers(2) GROUP BY ROLLUP(number, arrayCount(x -> x > 0, [number]) AS k, k + 1) ORDER BY ALL;

SELECT '-- lambda key captured by another lambda';
SELECT number, arrayMap(z -> arrayCount(x -> x > 0, [number]) + z, [10]) AS m, count() FROM numbers(2) GROUP BY ROLLUP(number, arrayCount(x -> x > 0, [number])) ORDER BY ALL;

SELECT '-- key reading a column that is not a key';
SELECT number, arrayCount(x -> x > c, [number]) AS k, count() FROM (SELECT number, number % 2 AS c FROM numbers(3)) GROUP BY ROLLUP(number, k) ORDER BY ALL;

SELECT '-- HAVING';
SELECT number, arrayCount(x -> x > 0, [number]) AS k, count() FROM numbers(3) GROUP BY ROLLUP(number, k) HAVING isNull(k) ORDER BY ALL;

SELECT '-- APPLY';
SELECT number, * APPLY (x -> arrayCount(y -> y > 0, [x])), count() FROM numbers(2) GROUP BY ROLLUP(number, arrayCount(y -> y > 0, [number])) ORDER BY ALL;

SELECT '-- lambda body folded to a constant';
SELECT number, arrayCount(x -> isNull(x), [number]) AS k, arrayCount(x -> isNotNull(x), [number]) AS other_folded, count() FROM numbers(2) GROUP BY ROLLUP(number, k) ORDER BY ALL;
SELECT number, arrayMap(x -> toTypeName(x), [number]) AS k, count() FROM numbers(2) GROUP BY ROLLUP(number, k) ORDER BY ALL;

SELECT '-- near misses are not the key';
SELECT number, arrayCount(x -> x > 0, [number]) AS k, arrayCount(x -> x > 1, [number]) AS other_constant, arrayCount(y -> y > 0, [number]) AS other_argument, arrayExists(x -> x > 0, [number]) AS other_function, arrayCount(x -> x > 0, [number, 1]) AS other_array, count() FROM numbers(2) GROUP BY ROLLUP(number, k) ORDER BY ALL;

SELECT '-- remote';
SELECT number, arrayCount(x -> x > 0, [number]) AS k, count() FROM remote('127.0.0.{1,2}', numbers(2)) GROUP BY ROLLUP(number, k) ORDER BY ALL;
