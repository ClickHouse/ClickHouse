-- An alias defined inside a lambda function that is called in the lambda function of `einsum` is local to the called function,
-- so it is neither an index of `einsum` nor a name from the place of the call. Every query is followed by its `arrayMap` equivalent.

-- The alias has the name of the index.
WITH x -> ((1 AS i) + i) AS f SELECT einsum(i -> a[i] + f(0)) FROM (SELECT [10, 20] AS a);
WITH x -> ((1 AS i) + i) AS f SELECT arrayMap(i -> a[i] + f(0), [1, 2]) FROM (SELECT [10, 20] AS a);

-- The alias is used in a lambda function nested in the called function.
WITH x -> arraySum(arrayMap(y -> y + i, [(x + 1) AS i, 5])) AS f SELECT einsum(i -> a[i] + f(0)) FROM (SELECT [10, 20] AS a);
WITH x -> arraySum(arrayMap(y -> y + i, [(x + 1) AS i, 5])) AS f SELECT arrayMap(i -> a[i] + f(0), [1, 2]) FROM (SELECT [10, 20] AS a);

-- The alias has the name of a parameter of a lambda function around the call.
WITH x -> ((2 AS v) * v) AS f SELECT einsum(i -> arraySum(arrayMap(v -> v + f(0), [1, 2])) + a[i]) FROM (SELECT [10, 20] AS a);
WITH x -> ((2 AS v) * v) AS f SELECT arrayMap(i -> arraySum(arrayMap(v -> v + f(0), [1, 2])) + a[i], [1, 2]) FROM (SELECT [10, 20] AS a);

-- The called function is called by another called function.
WITH x -> ((1 AS i) + i) AS f, y -> f(y) * 10 AS g SELECT einsum(i -> a[i] + g(0)) FROM (SELECT [10, 20] AS a);
WITH x -> ((1 AS i) + i) AS f, y -> f(y) * 10 AS g SELECT arrayMap(i -> a[i] + g(0), [1, 2]) FROM (SELECT [10, 20] AS a);

-- A local alias of a lambda function with the name of the index.
WITH x -> arrayMap((y -> y + 1) AS i, [x])[1] AS f SELECT einsum(i -> a[i] + f(0)) FROM (SELECT [10, 20] AS a);
WITH x -> arrayMap((y -> y + 1) AS i, [x])[1] AS f SELECT arrayMap(i -> a[i] + f(0), [1, 2]) FROM (SELECT [10, 20] AS a);

-- An alias local to a nested lambda function of the called function is not visible outside of it, so `i` after it is the index.
WITH x -> arraySum(arrayMap(y -> (y AS i), [1])) + i AS f SELECT einsum(i -> a[i] + f(0)) FROM (SELECT [10, 20] AS a); -- { serverError BAD_ARGUMENTS }

-- The alias does not bind its name inside its own expression, so `i` there is the index.
WITH x -> ((i + 1) AS i) AS f SELECT einsum(i -> a[i] + f(0)) FROM (SELECT [10, 20] AS a); -- { serverError BAD_ARGUMENTS }

-- A scalar alias with the name of a function does not hide the function: `get_a` in `f` is still the function that depends on `i`.
WITH x -> a[i] AS get_a, x -> ((1 AS get_a) + get_a(0)) AS f SELECT einsum(i -> a[i] + f(0)) FROM (SELECT [10, 20] AS a); -- { serverError BAD_ARGUMENTS }
