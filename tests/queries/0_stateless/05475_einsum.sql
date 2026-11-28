-- Einstein summation over arrays and fixed arrays.

SET enable_analyzer = 1;

SELECT '-- subscripts';
SELECT einsum('ik,kj->ij', [[1, 2], [3, 4]], [[5, 6], [7, 8]]) AS c, toTypeName(c);
SELECT einsum('ik,kj', [[1, 2], [3, 4]], [[5, 6], [7, 8]]);
SELECT einsum('i,i->', [1, 2, 3], [4, 5, 6]) AS c, toTypeName(c);
SELECT einsum('i,i', [1., 2, 3], [4, 5, 6]) AS c, toTypeName(c);
SELECT einsum('ij->ji', [[1, 2, 3], [4, 5, 6]]);
SELECT einsum('ij->ji', [['a', 'b', 'c'], ['d', 'e', 'f']]) AS c, toTypeName(c);
SELECT einsum('ii->i', [[1, 2], [3, 4]]), einsum('ii', [[1, 2], [3, 4]]), einsum('ii->', [[1, 2], [3, 4]]);
SELECT einsum('ij->', [[1, 2], [3, 4]]), einsum('ij->i', [[1, 2], [3, 4]]), einsum('ij->j', [[1, 2], [3, 4]]);
SELECT einsum('i,j->ij', [1, 2], [10, 20, 30]);
SELECT einsum('ij,ij->ij', [[1, 2], [3, 4]], [[5, 6], [7, 8]]);
SELECT einsum('bij,bjk->bik', [[[1, 0], [0, 1]], [[2, 0], [0, 2]]], [[[1, 2], [3, 4]], [[1, 2], [3, 4]]]);
SELECT einsum('ij,jk,kl->il', [[1, 2]], [[1, 0], [0, 1]], [[3], [4]]);
SELECT einsum('i,i,i->', [1, 2, 3], [1, 2, 3], [1, 2, 3]);
SELECT einsum('ijk->kji', [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]);
SELECT einsum('i->', [-1, -2, -3]::Array(Int8)) AS c, toTypeName(c);
SELECT einsum('i,i->', [1, 2]::Array(Float32), [3, 4]::Array(Float32)) AS c, toTypeName(c);
SELECT einsum('i,i->', [1, 2]::Array(Float32), [3, 4]::Array(UInt8)) AS c, toTypeName(c);
SELECT einsum('i,i->', [1, 2]::Array(Float32), [3, 4]::Array(Int64)) AS c, toTypeName(c);

SELECT '-- with a combiner';
SELECT einsum('ij,ij->ij', (x, y) -> x - y, [[1, 2], [3, 4]], [[5, 6], [7, 8]]);
SELECT einsum('i,i->', (x, y) -> (x - y) * (x - y), [1, 2, 3], [4, 6, 8]);
SELECT einsum('ij->ji', (x) -> concat(x, '!'), [['a', 'b'], ['c', 'd']]);
SELECT einsum('i->i', (x) -> x + number, [1, 2, 3]) FROM numbers(2);

SELECT '-- fixed arrays';
SELECT einsum('ik,kj->ij', a, b) AS c, toTypeName(c)
FROM (SELECT [[1, 2], [3, 4]]::FixedArray(2, 2, Float32) AS a, [[5, 6], [7, 8]]::FixedArray(2, 2, Float32) AS b);
SELECT einsum('ik,kj->ij', a, b) AS c, toTypeName(c)
FROM (SELECT [[1, 2], [3, 4]]::FixedArray(2, 2, Float32) AS a, [[5, 6], [7, 8]] AS b);
SELECT einsum('ik,kj->ij', a, b) AS c, toTypeName(c)
FROM (SELECT [[1, 2], [3, 4]]::FixedArray(2, 2, Float32) AS a, [[5, 6], [7, 8]]::Array(FixedArray(2, UInt8)) AS b);
SELECT einsum('ik,kj->ij', [[1, 2, 3]]::FixedArray(1, 3, UInt8), [[1], [2]]::FixedArray(2, 1, UInt8)); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

SELECT '-- lambda form';
SELECT einsum((i, j, k) -> a[i][k] * b[k][j], a, b) AS c, toTypeName(c)
FROM (SELECT [[1, 2], [3, 4]]::FixedArray(2, 2, Float32) AS a, [[5, 6], [7, 8]]::FixedArray(2, 2, Float32) AS b);
SELECT einsum((i, j, k) -> a[i, k] * b[k, j]) FROM (SELECT [[1, 2], [3, 4]] AS a, [[5, 6], [7, 8]] AS b);
SELECT einsum((i, j) -> a[j][i]) FROM (SELECT [[1, 2, 3], [4, 5, 6]] AS a);
SELECT einsum((i) -> a[i] * b[i], a, b) FROM (SELECT [1, 2, 3] AS a, [4, 5, 6] AS b);
SELECT einsum((i) -> a[i] * a[i]) FROM (SELECT [1, 2, 3] AS a);
SELECT einsum((i) -> a[i][i]) AS diagonal, arraySum(einsum((i) -> a[i][i])) AS trace FROM (SELECT [[1, 2], [3, 4]] AS a);
SELECT einsum((row, col) -> m[row][col] * 2) FROM (SELECT [[1, 2], [3, 4]] AS m);
SELECT einsum((i, j) -> a[i] * b[j]) FROM (SELECT [1, 2] AS a, [10, 20, 30] AS b);
SELECT einsum((i, k) -> a[i, k] * v[k] + 1) FROM (SELECT [[1, 2], [3, 4]] AS a, [10, 100] AS v);
SELECT einsum((i, j) -> 0.299 * image[i, j, 1] + 0.587 * image[i, j, 2] + 0.114 * image[i, j, 3]) AS gray, toTypeName(gray)
FROM (SELECT [[[255, 0, 0], [0, 255, 0]], [[0, 0, 255], [255, 255, 255]]]::FixedArray(2, 2, 3, UInt8) AS image);
SELECT einsum((i) -> m[i][n]) FROM (SELECT [[1, 2], [3, 4], [5, 6]] AS m, number + 1 AS n FROM numbers(2));
SELECT einsum((i, j) -> m[i][j] * w) FROM (SELECT [[1, 2], [3, 4]] AS m, number AS w FROM numbers(2));
SELECT formatQuery('SELECT einsum((i, j, k) -> a[i][k] * b[k][j])');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT einsum((i, j, k) -> a[i][k] * b[k][j]) FROM (SELECT [[1]] AS a, [[2]] AS b);
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT einsum((i, j) -> 0.5 * m[i, j, 1] + 0.5 * m[i, j, 2]) FROM (SELECT [[[1, 2]]] AS m);

SELECT '-- multiple rows and constants';
SELECT einsum('ik,kj->ij', a, [[1, 0], [0, 1]]) FROM (SELECT [[number, 1], [2, 3]] AS a FROM numbers(3));
SELECT einsum('i,i->', range(number), range(number)) FROM numbers(5);
SELECT einsum('ij,j->i', [[1, 2], [3, 4]], [number, 1]) FROM numbers(3);
SELECT einsum('ij->ji', arrayMap(i -> [i, i * 10], range(number))) FROM numbers(3);

SELECT '-- larger matrices are the same as the naive calculation';
WITH
    arrayMap(i -> arrayMap(j -> toFloat64((i * 7 + j * 3) % 11), range(70)), range(50)) AS a,
    arrayMap(i -> arrayMap(j -> toFloat64((i * 5 + j) % 13), range(60)), range(70)) AS b
SELECT einsum('ik,kj->ij', a, b) = arrayMap(i -> arrayMap(j -> arraySum(k -> a[i][k] * b[k][j], range(1, 71)), range(1, 61)), range(1, 51));
WITH
    arrayMap(i -> arrayMap(j -> toFloat64((i * 7 + j * 3) % 11), range(70)), range(50)) AS a,
    arrayMap(i -> arrayMap(j -> toFloat64((i * 5 + j) % 13), range(3)), range(70)) AS b
SELECT einsum('ik,kj->ij', a, b) = arrayMap(i -> arrayMap(j -> arraySum(k -> a[i][k] * b[k][j], range(1, 71)), range(1, 4)), range(1, 51));
WITH arrayMap(i -> arrayMap(j -> toInt64(i - j), range(40)), range(30)) AS a
SELECT einsum('ij,kj->ik', a, a) = arrayMap(i -> arrayMap(k -> arraySum(j -> a[i][j] * a[k][j], range(1, 41)), range(1, 31)), range(1, 31));

SELECT '-- errors';
SELECT einsum('ij,jk->ik', [[1, 2]], [[1, 2]]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT einsum('ij->ij', [[1, 2], [3]]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT einsum('ij', [1, 2]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT einsum('i,i', [1, 2]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT einsum('...i->i', [1, 2]); -- { serverError BAD_ARGUMENTS }
SELECT einsum('i->ii', [1, 2]); -- { serverError BAD_ARGUMENTS }
SELECT einsum('i->j', [1, 2]); -- { serverError BAD_ARGUMENTS }
SELECT einsum('i,i', ['a'], ['b']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT einsum(materialize('i'), [1]); -- { serverError ILLEGAL_COLUMN }
SELECT einsum((i) -> a[i] + i) FROM (SELECT [1, 2] AS a); -- { serverError BAD_ARGUMENTS }
SELECT einsum((i, j) -> a[i]) FROM (SELECT [1, 2] AS a); -- { serverError BAD_ARGUMENTS }
SELECT einsum((i) -> a[i + 1]) FROM (SELECT [1, 2] AS a); -- { serverError BAD_ARGUMENTS }
SELECT einsum((i) -> a[i], b) FROM (SELECT [1, 2] AS a, [3, 4] AS b); -- { serverError BAD_ARGUMENTS }
SELECT einsum((i) -> a[i] * b[i]) FROM (SELECT [1, 2] AS a, [3] AS b); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT einsum('i,i->', (x) -> x, [1], [2]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT einsum('i->', (x) -> toString(x), [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
