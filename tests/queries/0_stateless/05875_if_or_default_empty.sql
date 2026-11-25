-- https://github.com/ClickHouse/ClickHouse/issues/88829
SELECT avgIfOrDefault(number, number > 0 AND number < 60), isNaN(avgIf(number, number > 0 AND number < 60))
FROM (SELECT 1882.6168500000051 AS number);

SELECT avgIfOrDefault(number, number > 0 AND number < 60), isNaN(avgIf(number, number > 0 AND number < 60))
FROM (SELECT 0 AS number);

-- Empty input, rejected input, and admitted input.
SELECT avgIfOrDefault(number, 1) FROM numbers(0);
SELECT avgIfOrDefault(number, number > 100), avgIfOrDefault(number, number > 5), quantileIfOrDefault(0.9)(number, number > 100)
FROM numbers(10);

-- Both the small-key lookup table and the general grouped aggregation.
SELECT toUInt8(number % 3) AS k, avgIfOrDefault(number, number % 3 = 0)
FROM numbers(10) GROUP BY k ORDER BY k;

SELECT number % 3 AS k, avgIfOrDefault(number, number % 3 = 0)
FROM numbers(10) GROUP BY k ORDER BY k;

-- Nullable values and conditions must still count only admitted non-null rows.
SELECT
    avgIfOrDefault(toNullable(number), number > 100),
    avgIfOrDefault(number, toNullable(number > 100)),
    avgIfOrDefault(toNullable(number), toNullable(number > 100)),
    avgIfOrDefault(number, if(number % 2 = 0, NULL, number > 100))
FROM numbers(10);

SELECT avgIfOrDefault(if(number < 5, NULL, number), number < 5)
FROM numbers(10);

SELECT avgIfOrDefault(CAST(NULL AS Nullable(UInt64)), 1) FROM numbers(10);

-- Single-row updates through window functions and array aggregation.
SELECT DISTINCT avgIfOrDefault(number, number > 100) OVER (), avgIfOrDefault(number, number % 2 = 0) OVER ()
FROM numbers(10);

SELECT arrayReduce('avgIfOrDefault', [1, 2, 3], [0, 0, 0]);

-- An outer condition must be combined with the inner one.
SELECT avgIfOrDefaultIf(number, number % 2 = 0, number % 2 = 1), avgIfOrDefaultIf(number, number % 2 = 0, number < 5)
FROM numbers(10);

-- The presence flag must survive state finalization and merging.
SELECT finalizeAggregation(avgIfOrDefaultState(number, number > 100)) FROM numbers(10);

SELECT avgIfOrDefaultMerge(s)
FROM (SELECT avgIfOrDefaultState(number, number > 100) AS s FROM numbers(10) GROUP BY number % 3);

SELECT avgIfOrDefaultMerge(s)
FROM (SELECT avgIfOrDefaultState(number, number % 2 = 0) AS s FROM numbers(10) GROUP BY number % 3);

-- A NaN from an admitted value is a valid aggregate result.
SELECT isNaN(avgIfOrDefault(toFloat64('nan'), 1)), avgIfOrDefault(toFloat64('nan'), 0);
