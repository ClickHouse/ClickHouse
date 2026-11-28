-- The lambda form of `einsum`: the scopes of the names inside the lambda function.

-- The array and the subscripts that are not indices are computed outside of the lambda function,
-- so they must not depend on the parameters of nested lambda functions or on the aliases inside the lambda function.
SELECT einsum(i -> arraySum(arrayMap(a -> a[i], [[1, 2], [3, 4]]))) FROM (SELECT [10, 20] AS a); -- { serverError BAD_ARGUMENTS }
SELECT einsum(i -> arraySum(arrayMap(j -> a[i][j], [1, 2]))) FROM (SELECT [[1, 2], [3, 4]] AS a, 1 AS j); -- { serverError BAD_ARGUMENTS }
SELECT einsum(i -> ([5, 6] AS v)[1] + v[i]) FROM (SELECT [1, 2] AS v); -- { serverError BAD_ARGUMENTS }

-- The elements that do not depend on them are fine, also inside nested lambda functions.
SELECT einsum(i -> arraySum(arrayMap(x -> x * a[i], [1, 2]))) FROM (SELECT [1, 2] AS a);
SELECT einsum(i -> arrayMap(x -> x * 2, a)[i] * 3) FROM (SELECT [1, 2] AS a);
SELECT einsum(i -> arraySum(arrayMap(x -> a[i][j], [1]))) FROM (SELECT [[1, 2], [3, 4]] AS a, 2 AS j);

-- A scalar alias with the name `multiply` does not hide the function, so `i` is still a summation index.
SELECT einsum(i -> (1 AS multiply) + a[i] * a[i]) FROM (SELECT [1, 2] AS a);
SELECT einsum(i -> a[i] * a[i] + arraySum(arrayMap(x -> (x AS multiply), [0]))) FROM (SELECT [1, 2] AS a);

-- A lambda function with the alias `multiply` is called instead of the function in its scope, but not in other scopes.
SELECT einsum(i -> a[i] * a[i] + length(arrayMap(((y, z) -> y + z) AS multiply, [1], [2]))) FROM (SELECT [1, 2] AS a);
SELECT einsum(i -> a[i] * a[i] + arraySum(arrayMap(x -> length(arrayMap(((y, z) -> y + z) AS multiply, [x], [x])), [0]))) FROM (SELECT [1, 2] AS a);
