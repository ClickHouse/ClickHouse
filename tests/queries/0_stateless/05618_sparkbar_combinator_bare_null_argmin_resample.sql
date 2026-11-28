-- A bare `NULL` argument of `-Sparkbar` wrapped into `-ArgMin`, `-ArgMax` or `-Resample` behaves like a typed
-- `Nullable` `NULL`: the result keeps the `String` type of `-Sparkbar` instead of `0 :: UInt64`.

SELECT 'ArgMin, NULL key';
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT avgSparkbarArgMin(3, 0, 2)(NULL, number, number) AS r FROM numbers(3));
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT avgSparkbarArgMin(3, 0, 2)(CAST(NULL AS Nullable(UInt8)), number, number) AS r FROM numbers(3));
SELECT 'ArgMax, NULL argument';
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT avgSparkbarArgMax(3, 0, 2)(number, NULL, number) AS r FROM numbers(3));
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT avgSparkbarArgMax(3, 0, 2)(number, CAST(NULL AS Nullable(UInt8)), number) AS r FROM numbers(3));

SELECT 'Resample, NULL key';
SELECT toTypeName(r), r FROM (SELECT avgSparkbarResample(3, 0, 2, 0, 3, 1)(NULL, number, number) AS r FROM numbers(3));
SELECT toTypeName(r), r FROM (SELECT avgSparkbarResample(3, 0, 2, 0, 3, 1)(CAST(NULL AS Nullable(UInt8)), number, number) AS r FROM numbers(3));
SELECT 'Resample, NULL argument';
SELECT toTypeName(r), r FROM (SELECT avgSparkbarResample(3, 0, 2, 0, 3, 1)(number, NULL, number) AS r FROM numbers(3));

SELECT 'non-NULL rows are still aggregated';
SELECT avgSparkbarArgMax(3, 0, 2)(number, number, number) FROM numbers(3);
SELECT avgSparkbarResample(3, 0, 2, 0, 3, 3)(number, number, number) FROM numbers(3);
