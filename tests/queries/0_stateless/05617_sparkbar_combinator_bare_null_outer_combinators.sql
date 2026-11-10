-- A bare `NULL` argument of `-Sparkbar` wrapped into outer combinators (`-If`, `-Distinct`, `-State`, `-OrNull`)
-- yields the empty sparkbar of type `String`, the same as the unwrapped `-Sparkbar`, instead of `0 :: UInt64`.

SELECT 'If, NULL key';
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT avgSparkbarIf(3, 0, 2)(NULL, number, 1) AS r FROM numbers(3));
SELECT 'If, NULL argument';
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT avgSparkbarIf(3, 0, 2)(number, NULL, 1) AS r FROM numbers(3));
SELECT 'If, NULL condition';
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT avgSparkbarIf(3, 0, 2)(number, number, NULL) AS r FROM numbers(3));

SELECT 'Distinct, NULL key';
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT avgSparkbarDistinct(3, 0, 2)(NULL, number) AS r FROM numbers(3));
SELECT 'Distinct, NULL argument';
SELECT toTypeName(r), concat('[', r, ']') FROM (SELECT avgSparkbarDistinct(3, 0, 2)(number, NULL) AS r FROM numbers(3));

SELECT 'OrNull, NULL key';
SELECT toTypeName(r), r FROM (SELECT avgSparkbarOrNull(3, 0, 2)(NULL, number) AS r FROM numbers(3));

SELECT 'State, NULL key';
SELECT toTypeName(s), concat('[', finalizeAggregation(s), ']') FROM (SELECT avgSparkbarState(3, 0, 2)(NULL, number) AS s FROM numbers(3));
SELECT 'State, NULL argument';
SELECT concat('[', finalizeAggregation(s), ']') FROM (SELECT avgSparkbarState(3, 0, 2)(number, NULL) AS s FROM numbers(3));

SELECT 'non-NULL rows are still aggregated';
SELECT avgSparkbarIf(3, 0, 2)(number, number, number != 1) FROM numbers(3);
SELECT avgSparkbarDistinct(3, 0, 2)(number % 3, number % 3) FROM numbers(30);
