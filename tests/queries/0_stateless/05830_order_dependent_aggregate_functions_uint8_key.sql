-- Order-dependent aggregate functions see the rows of an ORDER BY subquery in order with a single 1-byte GROUP BY key,
-- whose lookup-table path splits each block between several tables.
-- https://github.com/ClickHouse/ClickHouse/issues/132708

SELECT k, deltaSum(v), deltaSumTimestamp(toUInt8(v), v)
FROM (SELECT toUInt8(number % 2) AS k, toUInt32(number) AS v FROM numbers(40) ORDER BY number)
GROUP BY k ORDER BY k
SETTINGS max_threads = 1;

SELECT k, any(v), anyLast(v), anyLastTuple(tuple(v))
FROM (SELECT toUInt8(number >= 6) AS k, toUInt32(number) AS v FROM numbers(16) ORDER BY number)
GROUP BY k ORDER BY k
SETTINGS max_threads = 1;

-- Several blocks.
SELECT k, any(v), anyLast(v), deltaSum(v)
FROM (SELECT toUInt8(number % 3) AS k, toUInt32(number) AS v FROM numbers(100000) ORDER BY number)
GROUP BY k ORDER BY k
SETTINGS max_threads = 1;
