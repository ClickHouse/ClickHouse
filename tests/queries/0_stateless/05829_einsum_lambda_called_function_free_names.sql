-- The lambda form of `einsum` must not change the bindings of the free names in the lambda functions called from the expression.

SET enable_analyzer = 1;

-- A free name of a called lambda function is bound to a parameter of a nested lambda function at the place of the call,
-- so the tensor element cannot be computed outside of the nested lambda function.
WITH x -> a[x] AS get_a SELECT einsum(i -> arraySum(arrayMap(a -> get_a(1)[i], [[[1, 2]], [[3, 4]]]))) FROM (SELECT [[100, 200]] AS a); -- { serverError BAD_ARGUMENTS }
WITH x -> a[x] AS get_a SELECT arrayMap(i -> arraySum(arrayMap(a -> get_a(1)[i], [[[1, 2]], [[3, 4]]])), [1, 2]) FROM (SELECT [[100, 200]] AS a);
-- Also transitively and when the function is passed to a higher-order function by its name.
WITH x -> a[x] AS get_a, y -> get_a(y) AS get_b SELECT einsum(i -> arraySum(arrayMap(a -> get_b(1)[i], [[[1, 2]], [[3, 4]]]))) FROM (SELECT [[100, 200]] AS a); -- { serverError BAD_ARGUMENTS }
WITH x -> a[x] AS get_a SELECT einsum(i -> arraySum(arrayMap(a -> arrayMap(get_a, [1])[1][i], [[[1, 2]], [[3, 4]]]))) FROM (SELECT [[100, 200]] AS a); -- { serverError BAD_ARGUMENTS }
-- The free name is a parameter of a lambda function inside the tensor element itself, so it is computed together with the element.
WITH x -> a[x] AS get_a SELECT einsum(i -> arrayMap(a -> get_a(1), [[[1, 2]]])[1][i]) FROM (SELECT [[100, 200]] AS a);
-- Without a nested lambda function the outer binding is kept.
WITH x -> a[x] AS get_a SELECT einsum(i -> get_a(1)[i]) FROM (SELECT [[100, 200]] AS a);

-- The generated parameters of the combining lambda function do not capture the free names of a function passed
-- to a higher-order function by its name.
WITH (x -> x + x1) AS g SELECT einsum(i -> arraySum(arrayMap(g, [a[i]]))) FROM (SELECT [1, 2] AS a, 10 AS x1);
WITH (x -> x + x1) AS g SELECT arrayMap(i -> arraySum(arrayMap(g, [a[i]])), [1, 2]) FROM (SELECT [1, 2] AS a, 10 AS x1);
