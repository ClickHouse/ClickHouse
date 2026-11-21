-- Empty buckets of `-Sparkbar` are rendered blank and do not affect scaling, even when the empty
-- state of the nested function does not produce zero (`groupBitAnd` of an empty set is all ones).

SELECT 'empty buckets';
SELECT concat('[', groupBitAndSparkbar(3, 0, 2)(number, toUInt8(1)), ']') FROM numbers(0);
SELECT concat('[', groupBitAndSparkbar(3, 0, 2)(number, toUInt8(1)), ']') FROM numbers(1);
SELECT concat('[', groupBitAndSparkbar(3, 0, 2)(number + 100, toUInt8(1)), ']') FROM numbers(5);
SELECT concat('[', groupBitAndSparkbar(3, 0, 2)(number, CAST(NULL AS Nullable(UInt8))), ']') FROM numbers(3);
SELECT concat('[', groupBitAndSparkbar(4, 0, 3)(number * 3, toUInt8(number + 1)), ']') FROM numbers(2);

SELECT 'occupancy survives merge and serialization';
SELECT concat('[', groupBitAndSparkbarMerge(4, 0, 3)(s), ']')
FROM (SELECT groupBitAndSparkbarState(4, 0, 3)(number * 3, toUInt8(number + 1)) AS s FROM numbers(2) GROUP BY number);
SELECT concat('[', finalizeAggregation(CAST(toString(s), toTypeName(s))), ']')
FROM (SELECT groupBitAndSparkbarState(4, 0, 3)(number * 3, toUInt8(number + 1)) AS s FROM numbers(2));

SELECT 'nullable inputs through outer combinators';
SELECT toTypeName(avgSparkbarIf(3, 0, 2)(toNullable(number), number, 1)) FROM numbers(3);
SELECT avgSparkbarIf(3, 0, 2)(toNullable(number), number, 1) FROM numbers(3);
SELECT concat('[', avgSparkbarIf(3, 0, 2)(CAST(NULL AS Nullable(UInt8)), number, 1), ']') FROM numbers(3);
SELECT toTypeName(avgSparkbarDistinct(3, 0, 2)(CAST(NULL AS Nullable(UInt8)), number)) FROM numbers(3);
SELECT concat('[', avgSparkbarDistinct(3, 0, 2)(CAST(NULL AS Nullable(UInt8)), number), ']') FROM numbers(3);
SELECT avgSparkbarDistinct(3, 0, 2)(toNullable(number % 3), number % 3) FROM numbers(30);

SELECT 'aggregate_functions_null_for_empty';
SELECT toTypeName(avgSparkbar(3, 0, 2)(number, number)) FROM numbers(3) SETTINGS aggregate_functions_null_for_empty = 1;
