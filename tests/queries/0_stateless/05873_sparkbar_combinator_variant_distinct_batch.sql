-- A bare `NULL` argument keeps the `String` result of `-Sparkbar` also when the function under an outer combinator
-- is wrapped into the adapter that converts `Variant` arguments: the result is the same as for a typed `Nullable` `NULL`.

SELECT 'Distinct around Sparkbar, NULL key, Variant value';
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT sumSparkbarDistinct(3, 0, 2)(NULL, CAST(number AS Variant(UInt64))) AS r FROM numbers(3));
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT sumSparkbarDistinct(3, 0, 2)(CAST(NULL AS Nullable(UInt64)), CAST(number AS Variant(UInt64))) AS r FROM numbers(3));
SELECT 'Distinct around Sparkbar, NULL key, Variant value, state';
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT finalizeAggregation(sumSparkbarDistinctState(3, 0, 2)(NULL, CAST(number AS Variant(UInt64)))) AS r FROM numbers(3));
SELECT 'Distinct around Sparkbar, Variant value';
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT sumSparkbarDistinct(3, 0, 2)(number, CAST(number + 1 AS Variant(UInt64))) AS r FROM numbers(3));

-- The rows are passed to the nested function of `-Sparkbar` in batches, so a nested `-Distinct` converts
-- its `Variant` arguments once per block. The result is the same as for the argument cast to `Nullable` explicitly,
-- and `-Distinct` still sees the original values.
SELECT 'Distinct inside Sparkbar, Variant value';
SELECT sumDistinctSparkbar(5, 0, 9)(number, v) = sumDistinctSparkbar(5, 0, 9)(number, CAST(v AS Nullable(UInt64))), sumDistinctSparkbar(5, 0, 9)(number, v)
FROM (SELECT number, CAST(if(number % 3 = 0, NULL, number) AS Variant(UInt64)) AS v FROM numbers(10));
SELECT 'Distinct inside Sparkbar, repeated values';
SELECT sumDistinctSparkbar(2, 0, 9)(number, v), sumSparkbar(2, 0, 9)(number, v)
FROM (SELECT number, CAST(if(number < 5, 1, number) AS Variant(UInt64)) AS v FROM numbers(10));
SELECT 'Distinct inside Sparkbar, different alternatives';
SELECT countDistinctSparkbar(2, 0, 9)(number, v)
FROM (SELECT number, if(number < 5, CAST(1 AS Variant(UInt8, String)), CAST(if(number % 2 = 0, '1', '2') AS Variant(UInt8, String))) AS v FROM numbers(10));

-- The batches are routed to the buckets of the place of every row.
SELECT 'GROUP BY';
SELECT k, sumDistinctSparkbar(5, 0, 9)(number, v) = sumDistinctSparkbar(5, 0, 9)(number, CAST(v AS Nullable(UInt64))), sumDistinctSparkbar(5, 0, 9)(number, v), sumSparkbar(5, 0, 9)(number, number)
FROM (SELECT number % 3 AS k, number, CAST(if(number % 4 = 0, NULL, number) AS Variant(UInt64)) AS v FROM numbers(10))
GROUP BY k ORDER BY k;
SELECT 'GROUP BY, UInt8 key';
SELECT k, sumSparkbar(5, 0, 9)(number, number), countSparkbarIf(5, 0, 9)(number, number % 2 = 0)
FROM (SELECT toUInt8(number % 3) AS k, number FROM numbers(10))
GROUP BY k ORDER BY k;
SELECT 'outer If and Nullable key';
SELECT sumDistinctSparkbarIf(5, 0, 9)(x, v, number < 8), sumSparkbarIf(5, 0, 9)(x, number, number < 8), sumSparkbar(5, 0, 9)(x, number)
FROM (SELECT number, if(number % 5 = 1, NULL, number) AS x, CAST(number AS Variant(UInt64)) AS v FROM numbers(10));
SELECT 'keys out of the range';
SELECT sumSparkbar(5, 2, 6)(number, number), sumDistinctSparkbar(5, 2, 6)(number, CAST(number AS Variant(UInt64))) FROM numbers(10);
SELECT 'window';
SELECT number, sumDistinctSparkbar(3, 0, 5)(number, v) OVER (ORDER BY number ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
FROM (SELECT number, CAST(number + 1 AS Variant(UInt64)) AS v FROM numbers(6)) ORDER BY number;
