-- Regressions for `FixedArray`, partial subscripts and `einsum`: a function that returns an element of
-- a multidimensional `FixedArray`, aliases inside the lambda form of `einsum` and inside chains of subscripts,
-- `joinGet` with `FixedArray` keys, `tupleElement` of `FixedArray` of tuples, and the sizes for `replicate`.

SET enable_analyzer = 1;
SET allow_experimental_fixed_array_type = 1;

SELECT '-- element of a multidimensional FixedArray';
SELECT arrayMap(x -> arrayLast(y -> y[2] > 0, x), CAST([[[1, 2, 3], [4, 5, 6]]], 'FixedArray(1, 2, 3, UInt8)')) AS r, toTypeName(r);
SELECT arrayFirst(y -> y[1] = 4, CAST([[1, 2, 3], [4, 5, 6]], 'FixedArray(2, 3, UInt8)')) AS r, toTypeName(r);
SELECT arrayLast(y -> y[materialize(2)] > 0, materialize(CAST([[1, 2, 3], [4, 5, 6]], 'FixedArray(2, 3, UInt8)'))) AS r, toTypeName(r);

SELECT '-- einsum alias in a product';
SELECT einsum(i -> a[i] * a[i]) FROM (SELECT [1, 2] AS a);
SELECT einsum(i -> (a[i] AS v) * v) FROM (SELECT [1, 2] AS a);
SELECT einsum(i -> ((a[i] AS v) + 1 AS w) * w) FROM (SELECT [1, 2] AS a);

SELECT '-- intermediate aliases in subscripts';
SELECT ([[1, 2], [3, 4]][1] AS r), r;
SELECT ([[1, 2], [3, 4]][1] AS r)[], r;
SELECT ([[1, 2], [3, 4]] AS r)[][2], r;
SELECT ([[1, 2], [3, 4]][] AS r)[2], r;

SELECT '-- alias matching a generated name';
SELECT [[1, 2], [3, 4]][][(1 AS x)];
SELECT [[1, 2], [3, 4]][][(2 AS x)] AS x1;

SELECT '-- joinGet';
DROP TABLE IF EXISTS j_fixed_array;
CREATE TABLE j_fixed_array (k FixedArray(2, UInt8), v UInt8) ENGINE = Join(ANY, LEFT, k);
INSERT INTO j_fixed_array VALUES ([1, 2], 42);
SELECT joinGet('j_fixed_array', 'v', [1, 2]::FixedArray(2, UInt8));
SELECT joinGetOrNull('j_fixed_array', 'v', [1, 3]::FixedArray(2, UInt8));
DROP TABLE j_fixed_array;

SELECT '-- tupleElement';
SELECT tupleElement([(1, 'a'), (2, 'b')]::FixedArray(2, Tuple(UInt8, String)), 1) AS r, toTypeName(r);
SELECT tupleElement(materialize([(1, 'a'), (2, 'b')]::FixedArray(2, Tuple(UInt8, String))), 2) AS r, toTypeName(r);
SELECT tupleElement([[(1, 'a')], [(2, 'b')]]::FixedArray(2, 1, Tuple(UInt8, String)), 1) AS r, toTypeName(r);
SELECT tupleElement([[(1, 'a')], [(2, 'b')], [(3, 'c')]]::Array(FixedArray(1, Tuple(UInt8, String))), 1) AS r, toTypeName(r);
SELECT tupleElement([([1, 2]::FixedArray(2, UInt8), 'a')], 1) AS r, toTypeName(r);

SELECT '-- replicate';
SELECT replicate(9, [1, 2]::FixedArray(2, UInt8)) AS r, toTypeName(r);
SELECT replicate(9, materialize([1, 2]::FixedArray(2, UInt8))) AS r, toTypeName(r);
SELECT replicate([1, 2]::FixedArray(2, UInt8), [1, 2, 3]) AS r, toTypeName(r);
SELECT replicate([1, 2]::FixedArray(2, UInt8), [1, 2, 3]::FixedArray(3, UInt8)) AS r, toTypeName(r);
