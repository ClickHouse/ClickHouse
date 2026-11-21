-- A bare `NULL` in the trailing argument of `-ArgMin`, `-ArgMax` or `-Resample` around `-Sparkbar` behaves like a typed
-- `Nullable` `NULL`: the result keeps the `String` type of `-Sparkbar` instead of `0 :: UInt64` or an exception.

SELECT 'ArgMin, NULL comparator';
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT avgSparkbarArgMin(3, 0, 2)(number, number, NULL) AS r FROM numbers(3));
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT avgSparkbarArgMin(3, 0, 2)(number, number, CAST(NULL AS Nullable(UInt8))) AS r FROM numbers(3));
SELECT 'ArgMax, NULL comparator';
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT avgSparkbarArgMax(3, 0, 2)(number, number, NULL) AS r FROM numbers(3));
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT avgSparkbarArgMax(3, 0, 2)(number, number, CAST(NULL AS Nullable(UInt8))) AS r FROM numbers(3));
SELECT 'ArgMin, NULL comparator, state';
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT finalizeAggregation(avgSparkbarArgMinState(3, 0, 2)(number, number, NULL)) AS r FROM numbers(3));

SELECT 'Resample, NULL key';
SELECT toTypeName(r), r FROM (SELECT avgSparkbarResample(3, 0, 2, 0, 3, 1)(number, number, NULL) AS r FROM numbers(3));
SELECT toTypeName(r), r FROM (SELECT avgSparkbarResample(3, 0, 2, 0, 3, 1)(number, number, CAST(NULL AS Nullable(UInt8))) AS r FROM numbers(3));
SELECT 'Resample, NULL key, signed bounds';
SELECT toTypeName(r), r FROM (SELECT avgSparkbarResample(3, 0, 2, -1, 2, 1)(number, number, NULL) AS r FROM numbers(3));
SELECT toTypeName(r), r FROM (SELECT avgSparkbarResample(3, 0, 2, -1, 2, 1)(number, number, CAST(NULL AS Nullable(Int8))) AS r FROM numbers(3));

SELECT 'other nested functions are not affected';
SELECT toTypeName(r), r FROM (SELECT sumArgMin(number, NULL) AS r FROM numbers(3));
SELECT toTypeName(r), r FROM (SELECT countArgMin(number, NULL) AS r FROM numbers(3));
SELECT sumResample(0, 3, 1)(number, NULL) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A forwarded `Variant` argument is converted around `-Sparkbar`, once for the whole block:
-- the result is the same as for the argument cast to `Nullable` explicitly.
SELECT 'Variant argument';
SELECT sumSparkbar(5, 0, 9)(number, v) = sumSparkbar(5, 0, 9)(number, CAST(v AS Nullable(UInt64))), sumSparkbar(5, 0, 9)(number, v)
FROM (SELECT number, CAST(if(number % 3 = 0, NULL, number) AS Variant(UInt64)) AS v FROM numbers(10));
SELECT 'Variant argument under an outer combinator';
SELECT sumSparkbarIf(5, 0, 9)(number, v, number < 8) = sumSparkbarIf(5, 0, 9)(number, CAST(v AS Nullable(UInt64)), number < 8)
FROM (SELECT number, CAST(if(number % 3 = 0, NULL, number) AS Variant(UInt64)) AS v FROM numbers(10));
