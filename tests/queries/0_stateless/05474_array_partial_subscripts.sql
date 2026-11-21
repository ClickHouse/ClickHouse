-- Several subscripts arr[i, j] and the subscript of all elements arr[][j].

SELECT '-- formatting';
SELECT formatQuery('SELECT arr[i, j]');
SELECT formatQuery('SELECT arr[i, j, k][l]');
SELECT formatQuery('SELECT arr[][1]');
SELECT formatQuery('SELECT arr[][][1]');
SELECT formatQuery('SELECT arr[1][][2, 3]');
SELECT formatQuery('SELECT arr[][x]');
SELECT formatQuery('SELECT arr[]');
SELECT formatQuery('SELECT arr[][1] AS y, arr[1, 2] AS z');
SELECT formatQuery('SELECT f(arr[][1])[2]');

SELECT '-- arrays';
SELECT [[1, 2, 3], [4, 5, 6]] AS m, m[2, 3], m[1, -1], m[][2], m[][-1], m[];
SELECT [[[1, 2], [3, 4]], [[5, 6], [7, 8]]] AS t, t[][][1], t[][2][1], t[2][][2], t[2, 1, 2];
SELECT [[1, 2, 3], [4, 5, 6]] AS m, number + 1 AS x, m[][x] FROM numbers(3);

SELECT '-- fixed arrays keep the sizes';
SELECT [[[1, 2, 3], [4, 5, 6]]]::FixedArray(1, 2, 3, UInt8) AS image, image[][][1] AS red, toTypeName(red), image[1, 2], toTypeName(image[1, 2]);
SELECT 0.299 * image[][][1] + 0.587 * image[][][2] + 0.114 * image[][][3] AS gray, toTypeName(gray)
FROM (SELECT [[[255, 0, 0], [0, 255, 0]], [[0, 0, 255], [255, 255, 255]]]::FixedArray(2, 2, 3, UInt8) AS image);
