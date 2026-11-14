-- Bar levels of integer results of `-Sparkbar` must be exact even when the product of a result and
-- the number of levels overflows the result type: 37 and 38 must render as levels 7 and 8.
SELECT maxSparkbar(2, 0, 1)(number, toUInt8(37 + number)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toInt8(126 + number)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, if(number = 0, toUInt8(128), toUInt8(255))) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toUInt64(18446744073709551614) + number) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toInt256('57896044618658097711785492504343953926634992332820282019728792003956564819966') + number) FROM numbers(2);
SELECT maxSparkbar(8, 0, 7)(number, toUInt8(32 * number + 31)) FROM numbers(8);

-- The state type of `-Sparkbar` keeps the explicit state version of the nested function.
SELECT toTypeName(uniqSparkbarState(2, 0, 1)(number, number)) FROM numbers(2);
SELECT toTypeName(quantileDeterministicSparkbarState(0.5, 2, 0, 1)(number, number, number)) FROM numbers(2);

-- So the result does not change after a round trip of the states through `groupArray`.
WITH src AS
(
    SELECT toUInt64(0) AS x, number AS v, number AS d, toUInt8(number >= 990000) AS part FROM numbers(1000000)
    UNION ALL
    SELECT toUInt64(1), toUInt64(1750000), toUInt64(1000000), toUInt8(2)
)
SELECT
    (SELECT quantileDeterministicSparkbar(0.5, 2, 0, 1)(x, v, d) FROM src),
    (SELECT quantileDeterministicSparkbarMerge(0.5, 2, 0, 1)(arrayJoin(states))
     FROM (SELECT groupArray(s) AS states FROM
         (SELECT quantileDeterministicSparkbarState(0.5, 2, 0, 1)(x, v, d) AS s FROM src GROUP BY part)));
