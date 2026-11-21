-- Regressions for `FixedArray`, the subscripts `arr[][j]` and the lambda form of `einsum`: lambda functions passed as parameters
-- of nested lambda functions, free names of the called lambda functions, the aliases inside subqueries, lazy arguments of
-- short-circuit functions and string constants compared with `FixedArray` keys.

SET enable_analyzer = 1;
SET allow_experimental_fixed_array_type = 1;

-- A call of a parameter of a nested lambda function is a local name: the array of a tensor element cannot be computed
-- outside of the lambda function.
WITH x -> [100, 200] AS f, (g, h) -> g(h) AS invoke SELECT einsum(i -> invoke((f -> f(0)[i]), (x -> [1, 2]))); -- { serverError BAD_ARGUMENTS }
WITH x -> [100, 200] AS f, (g, h) -> g(h) AS invoke SELECT arrayMap(i -> invoke((f -> f(0)[i]), (x -> [1, 2])), [1, 2]);

-- The generated parameters do not capture the free names of the called lambda functions.
WITH (x -> x + x1) AS f SELECT einsum(i -> f(a[i])) FROM (SELECT [1, 2] AS a, 10 AS x1);
WITH y -> y + x AS f SELECT [[10, 20], [30, 40]][][f(0)] FROM (SELECT 1 AS x);
WITH y -> y + x AS f SELECT arrayMap(z -> z[f(0)], [[10, 20], [30, 40]]) FROM (SELECT 1 AS x);
WITH y -> y + x AS f SELECT [[[1, 2], [3, 4]], [[5, 6], [7, 8]]][][f(1)][f(0)], [[[1, 2], [3, 4]], [[5, 6], [7, 8]]][][][f(0)] FROM (SELECT 1 AS x);
SELECT formatQuery('SELECT arr[][f(0)][], arr[1][][g(x)][][2] AS r, arr[][x]');
WITH y -> y + x AS f SELECT einsum(i -> a[i][f(0)]) FROM (SELECT [[1, 2], [3, 4]] AS a, 1 AS x);
WITH y -> y + x AS f SELECT einsum(i -> a[i][f(0)] + 0) FROM (SELECT [[1, 2], [3, 4]] AS a, 1 AS x);

-- An alias inside a subquery does not shadow the builtin `multiply` in the lambda function of `einsum`.
SELECT einsum(i -> length((SELECT arrayMap(((x, y) -> x + y) AS multiply, [0], [0]))) + a[i] * a[i]) FROM (SELECT [1, 2] AS a);
SELECT einsum(i -> length((SELECT arrayMap(((x, y) -> x + y) AS f, [0], [0]))) + a[i] * a[i]) FROM (SELECT [1, 2] AS a);

-- A lazy argument of a short-circuit function with the `FixedArray` result.
SELECT CASE number WHEN 0 THEN CAST([number], 'FixedArray(1, UInt64)') ELSE CAST([number + 1], 'FixedArray(1, UInt64)') END
FROM numbers(2) SETTINGS short_circuit_function_evaluation = 'force_enable';
SELECT CASE number WHEN 0 THEN CAST([number], 'FixedArray(1, UInt64)') ELSE CAST([number + 1], 'FixedArray(1, UInt64)') END
FROM numbers(4) SETTINGS short_circuit_function_evaluation = 'enable';
SELECT if(number % 2 = 0, CAST([number], 'FixedArray(1, UInt64)'), CAST([number + 10], 'FixedArray(1, UInt64)'))
FROM numbers(4) SETTINGS short_circuit_function_evaluation = 'force_enable';

-- A string constant compared with a `FixedArray` key is parsed with the integer elements of the same range as at runtime.
DROP TABLE IF EXISTS t_fixed_array_string_constant;
CREATE TABLE t_fixed_array_string_constant (a FixedArray(1, UInt8)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_fixed_array_string_constant VALUES ([0]), ([1]), ([255]);
SELECT a FROM t_fixed_array_string_constant WHERE a < '[256]' ORDER BY a SETTINGS use_primary_key = 1;
SELECT a FROM t_fixed_array_string_constant WHERE a < '[256]' ORDER BY a SETTINGS use_primary_key = 0;
SELECT a FROM t_fixed_array_string_constant WHERE a > '[-1]' ORDER BY a SETTINGS use_primary_key = 1;
SELECT a FROM t_fixed_array_string_constant WHERE a > '[-1]' ORDER BY a SETTINGS use_primary_key = 0;
DROP TABLE t_fixed_array_string_constant;
