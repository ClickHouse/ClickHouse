-- Regressions for the review of the lambda form of `einsum` and of `einsum` with a combining lambda function.

SET enable_analyzer = 1;

-- A free name of a called lambda function is bound to an index of `einsum` at the place of the call,
-- so the function cannot be computed outside of the lambda function of `einsum`.
WITH x -> a[i] AS get_a SELECT einsum(i -> get_a(0)[i]) FROM (SELECT [[1, 2], [3, 4]] AS a, 1 AS i); -- { serverError BAD_ARGUMENTS }
WITH x -> a[i] AS get_a SELECT arrayMap(i -> get_a(0)[i], [1, 2]) FROM (SELECT [[1, 2], [3, 4]] AS a, 1 AS i);
-- The same for a subscript that is not an index.
WITH x -> i AS idx SELECT einsum(i -> a[i][idx(0)]) FROM (SELECT [[1, 2], [3, 4]] AS a, 1 AS i); -- { serverError BAD_ARGUMENTS }
WITH x -> i AS idx SELECT arrayMap(i -> a[i][idx(0)], [1, 2]) FROM (SELECT [[1, 2], [3, 4]] AS a, 1 AS i);
-- Also transitively, when the function is passed by its name, and outside of the elements of tensors.
WITH x -> a[i] AS get_a, y -> get_a(y) AS get_b SELECT einsum(i -> get_b(0)[i]) FROM (SELECT [[1, 2], [3, 4]] AS a, 1 AS i); -- { serverError BAD_ARGUMENTS }
WITH x -> a[i] AS get_a SELECT einsum(i -> arrayMap(get_a, [0])[1][i]) FROM (SELECT [[1, 2], [3, 4]] AS a, 1 AS i); -- { serverError BAD_ARGUMENTS }
WITH x -> b[i] AS get_b SELECT einsum(i -> a[i] + get_b(0)) FROM (SELECT [1, 2] AS a, [10, 20] AS b, 1 AS i); -- { serverError BAD_ARGUMENTS }
-- A function that does not depend on the indices is computed outside as before.
WITH x -> a[x] AS get_a SELECT einsum(i -> get_a(2)[i]) FROM (SELECT [[1, 2], [3, 4]] AS a, 1 AS i);
WITH x -> b[x] AS get_b SELECT einsum(i -> a[i] + get_b(2)) FROM (SELECT [1, 2] AS a, [10, 20] AS b, 1 AS i);

-- A parameter of a lambda function that is an argument of a builtin function is a value,
-- so it does not hide the builtin `multiply`, and its name does not change the summation.
SELECT einsum(i -> arraySum(arrayMap(multiply -> a[i] * a[i], [0]))) FROM (SELECT [1, 2] AS a);
SELECT einsum(i -> arraySum(arrayMap(unrelated -> a[i] * a[i], [0]))) FROM (SELECT [1, 2] AS a);
SELECT arrayMap(multiply -> 2 * 3, [0]);
-- A parameter of a lambda function passed to a lambda function can be a function, so the form is rejected as ambiguous.
WITH (g, h) -> g(h) AS invoke SELECT einsum(i -> invoke(multiply -> a[i] * a[i], 0)) FROM (SELECT [1, 2] AS a); -- { serverError BAD_ARGUMENTS }
WITH (g, h) -> g(h) AS invoke SELECT einsum(i -> invoke(unrelated -> a[i] * a[i], 0)) FROM (SELECT [1, 2] AS a);

-- The combining lambda function gets the elements without `LowCardinality`, as in `arrayMap`.
SELECT einsum('i->i', (x) -> concat(x, '!'), ['a', 'b']::Array(LowCardinality(String)));
SELECT arrayMap(x -> concat(x, '!'), ['a', 'b']::Array(LowCardinality(String)));
SELECT einsum('i,i->i', (x, y) -> concat(x, y), ['a', 'b']::Array(LowCardinality(String)), ['c', 'd']::FixedArray(2, LowCardinality(String)));
SELECT einsum('ij,j->ij', (x, y) -> concat(x, y), [['a', 'b'], ['c', 'd']]::FixedArray(2, Array(LowCardinality(String))), ['x', 'y']);
SELECT einsum('i->i', (x) -> toTypeName(x), materialize(['a', 'b']::Array(LowCardinality(Nullable(String)))));
SELECT einsum('i->i', (x) -> toTypeName(x), ['a', 'b']::FixedArray(2, LowCardinality(String)));
