-- Regressions for `FixedArray` and the lambda form of `einsum`: a constant `FixedArray` captured by a lambda function,
-- `Variant` tensors with a combiner lambda, scopes of aliases inside nested lambda functions, lambda functions bound
-- to the names `einsum` and `multiply`, and dynamic subcolumns of `FixedArray(N, JSON)` and `FixedArray(N, Dynamic)`.

SET enable_analyzer = 1;
SET allow_experimental_fixed_array_type = 1;

SELECT '-- captured constant FixedArray';
SELECT arrayMap(r -> defaultValueOfArgumentType(r), CAST('[[1, 2], [3, 4]]', 'FixedArray(2, 2, UInt8)')) AS r, toTypeName(r);
SELECT arrayMap(r -> defaultValueOfArgumentType(r), materialize(CAST('[[1, 2], [3, 4]]', 'FixedArray(2, 2, UInt8)'))) AS r;
SELECT arrayMap(r -> defaultValueOfArgumentType(r), CAST('[[1, 2], [3, 4]]', 'FixedArray(2, 2, UInt8)')) AS mm GROUP BY 1 WITH ROLLUP ORDER BY ALL;

SELECT '-- Variant tensors';
SELECT einsum(i -> intDiv(a[i], b[i]), a, b) FROM (SELECT [1, 2, 3]::Variant(Array(Int64), Array(UInt64)) AS b, [4, 5, 6] AS a); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT einsum('i,i->i', (x, y) -> x + y, [1, 2], [1, 2]::Variant(Array(Int64), Array(UInt64))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT einsum('i,i->i', [1, 2], [1, 2]::Variant(Array(Int64), Array(UInt64)));

SELECT '-- scopes of aliases';
SELECT einsum(i -> a[i] * v + arraySum(arrayMap(x -> (a[i] AS v), [0]))) FROM (SELECT [1, 2] AS a, 10 AS v);
SELECT einsum(i -> (a[i] AS v) + arraySum(arrayMap(v -> a[i] * v, [1]))) FROM (SELECT [1, 2] AS a);
SELECT einsum(i -> (a[i] AS v) * v) FROM (SELECT [1, 2] AS a);
SELECT einsum(i -> arraySum(arrayMap(x -> (a[i] AS v) * v, [1]))) FROM (SELECT [1, 2] AS a);

SELECT '-- scoped functions';
WITH (f -> arrayMap(f, [1, 2])) AS einsum SELECT einsum(x -> x + 1);
WITH (x, y) -> x + y AS multiply SELECT einsum(i -> multiply(a[i], a[i])) FROM (SELECT [1, 2] AS a);
WITH (x, y) -> x + y AS multiply SELECT einsum(i -> a[i] * a[i]) FROM (SELECT [1, 2] AS a);
SELECT einsum(i -> multiply(a[i], a[i])) FROM (SELECT [1, 2] AS a);
SELECT einsum(i -> a[i] * a[i]) FROM (SELECT [1, 2] AS a, 3 AS multiply);

SELECT '-- dynamic subcolumns';
SELECT getSubcolumn(CAST(['{"x":1}', '{"x":2}'], 'FixedArray(2, JSON)'), 'x') AS r, toTypeName(r);
SELECT CAST(['{"x":1}', '{"x":2}'], 'FixedArray(2, JSON)') AS a, a.x;

DROP TABLE IF EXISTS t_fixed_array_json;
CREATE TABLE t_fixed_array_json (a FixedArray(2, JSON), d FixedArray(2, Dynamic)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_fixed_array_json VALUES (['{"x":1,"y":"a"}', '{"x":2}'], [1, 'a']);
SELECT a.x, a.y, toTypeName(a.x), d.Int64, d.String, toTypeName(d.Int64) FROM t_fixed_array_json;
SELECT a.x FROM t_fixed_array_json;
DROP TABLE t_fixed_array_json;
