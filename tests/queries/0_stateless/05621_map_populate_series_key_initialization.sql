-- Exercise consecutive output ranges with empty rows, gaps, truncation and duplicate keys.
SELECT
    id,
    mapPopulateSeries(k, v, b) = (ek, ev) AS arrays_ok,
    mapPopulateSeries(mapFromArrays(k, v), b) = mapFromArrays(ek, ev) AS map_ok
FROM values('id UInt8, k Array(Int64), v Array(Int64), b Int64, ek Array(Int64), ev Array(Int64)',
    (0, [], [], 5, [], []),
    (1, [7, 9], [70, 90], 10, [7, 8, 9, 10], [70, 0, 90, 0]),
    (2, [100], [1], 99, [], []),
    (3, [-2, 0], [2, 4], 1, [-2, -1, 0, 1], [2, 0, 4, 0]),
    (4, [9, 7, 9], [92, 70, 91], 10, [7, 8, 9, 10], [70, 0, 92, 0]),
    (5, [9223372036854775805, 9223372036854775807], [5, 6], 9223372036854775807,
        [9223372036854775805, 9223372036854775806, 9223372036854775807], [5, 0, 6]),
    (6, [], [], -1, [], []),
    (7, [42], [9], 42, [42], [9]))
ORDER BY id;

WITH
    toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639933') AS first_key,
    toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639934') AS middle_key,
    toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935') AS last_key,
    materialize([first_key, last_key]) AS k,
    materialize([toUInt8(11), toUInt8(22)]) AS v,
    materialize(last_key) AS b,
    [first_key, middle_key, last_key] AS ek,
    [toUInt8(11), toUInt8(0), toUInt8(22)] AS ev
SELECT
    'UInt256-max',
    mapPopulateSeries(k, v, b) = (ek, ev) AS arrays_ok,
    mapPopulateSeries(mapFromArrays(k, v), b) = mapFromArrays(ek, ev) AS map_ok;

WITH
    toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968') AS first_key,
    toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819967') AS middle_key,
    toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819966') AS last_key,
    materialize([first_key, last_key]) AS k,
    materialize([toUInt8(11), toUInt8(22)]) AS v,
    materialize(last_key) AS b,
    [first_key, middle_key, last_key] AS ek,
    [toUInt8(11), toUInt8(0), toUInt8(22)] AS ev
SELECT
    'Int256-min',
    mapPopulateSeries(k, v, b) = (ek, ev) AS arrays_ok,
    mapPopulateSeries(mapFromArrays(k, v), b) = mapFromArrays(ek, ev) AS map_ok;
