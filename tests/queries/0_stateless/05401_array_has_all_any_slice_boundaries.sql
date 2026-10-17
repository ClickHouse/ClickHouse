-- Arrays around the sizes where `hasAll` and `hasAny` switch between scalar, vector and block search, with the needle at each position and one past the end.
SELECT 'UInt8', count(), countIf(hasAny(arr, [x]) != arrayExists(e -> e = x, arr) OR hasAll(arr, [x, toUInt8(0)]) != (arrayExists(e -> e = x, arr) AND has(arr, 0)))
FROM (SELECT arrayMap(i -> toUInt8(i), range(n)) AS arr, toUInt8(p) AS x FROM (SELECT arrayJoin([1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 160, 255]) AS n) ARRAY JOIN range(n + 1) AS p);

SELECT 'Int16', count(), countIf(hasAny(arr, [x]) != arrayExists(e -> e = x, arr) OR hasAll(arr, [x, toInt16(0)]) != (arrayExists(e -> e = x, arr) AND has(arr, 0)))
FROM (SELECT arrayMap(i -> toInt16(i), range(n)) AS arr, toInt16(p) AS x FROM (SELECT arrayJoin([1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 200]) AS n) ARRAY JOIN range(n + 1) AS p);

SELECT 'Int64', count(), countIf(hasAny(arr, [x]) != arrayExists(e -> e = x, arr) OR hasAll(arr, [x, toInt64(0)]) != (arrayExists(e -> e = x, arr) AND has(arr, 0)))
FROM (SELECT arrayMap(i -> toInt64(i), range(n)) AS arr, toInt64(p) AS x FROM (SELECT arrayJoin([1, 3, 4, 5, 15, 16, 17, 19, 20, 31, 32, 33, 40, 64]) AS n) ARRAY JOIN range(n + 1) AS p);

SELECT 'Nullable Int32', count(), countIf(hasAny(arr, [x]) != arrayExists(e -> e = x, arr))
FROM (SELECT arrayMap(i -> if(i % 7 = 3, NULL, toInt32(i)), range(n)) AS arr, toNullable(toInt32(p)) AS x FROM (SELECT arrayJoin([5, 8, 9, 16, 17, 33, 64]) AS n) ARRAY JOIN range(n + 1) AS p);
