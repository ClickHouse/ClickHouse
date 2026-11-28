-- Test sumSparkbar combinator with specified range
SELECT 'sumSparkbar with range:';
SELECT sumSparkbar(10, 0, 9)(number, number + 1) FROM numbers(10);

-- Test countSparkbar combinator with specified range
SELECT 'countSparkbar with range:';
SELECT countSparkbar(10, 0, 9)(number) FROM numbers(10);

-- Test avgSparkbar combinator with specified range
SELECT 'avgSparkbar with range:';
SELECT avgSparkbar(5, 0, 4)(number, number * 2) FROM numbers(5);

-- Test minSparkbar combinator
SELECT 'minSparkbar with range:';
SELECT minSparkbar(5, 0, 4)(number, 10 - number) FROM numbers(5);

-- Test maxSparkbar combinator
SELECT 'maxSparkbar with range:';
SELECT maxSparkbar(5, 0, 4)(number, number) FROM numbers(5);

-- Test with Date type
SELECT 'sumSparkbar with Date:';
SELECT sumSparkbar(5, toDate('2020-01-01'), toDate('2020-01-05'))(
    toDate('2020-01-01') + number,
    number + 1
) FROM numbers(5);

-- Test with multiple rows per bucket
SELECT 'sumSparkbar multiple rows per bucket:';
SELECT sumSparkbar(5, 0, 4)(number % 5, 1) FROM numbers(20);

-- Test with GROUP BY
SELECT 'sumSparkbar with GROUP BY:';
SELECT
    number % 2 AS grp,
    sumSparkbar(5, 0, 4)(number DIV 2, number)
FROM numbers(10)
GROUP BY grp
ORDER BY grp;

-- Test empty result (all values out of range)
SELECT 'sumSparkbar empty (out of range):';
SELECT sumSparkbar(5, 100, 104)(number, 1) FROM numbers(10);

-- Test with uniqSparkbar
SELECT 'uniqSparkbar with range:';
SELECT uniqSparkbar(5, 0, 4)(number, number % 3) FROM numbers(15);

-- Test anySparkbar
SELECT 'anySparkbar with range:';
SELECT anySparkbar(5, 0, 4)(number, number + 1) FROM numbers(5);

-- Test State/Merge round-trip (serialization/deserialization)
SELECT 'State/Merge round-trip:';
SELECT sumSparkbarMerge(5, 0, 4)(state)
FROM (
    SELECT sumSparkbarState(5, 0, 4)(number, number + 1) AS state
    FROM numbers(5)
);

-- Test merging from multiple sources (distributed query simulation)
SELECT 'Merge from multiple sources:';
SELECT sumSparkbarMerge(5, 0, 9)(state)
FROM (
    SELECT sumSparkbarState(5, 0, 9)(number, 1) AS state FROM numbers(5)
    UNION ALL
    SELECT sumSparkbarState(5, 0, 9)(number + 5, 1) AS state FROM numbers(5)
);

-- Test finalizeAggregation with State (alternative to AggregatingMergeTree)
SELECT 'finalizeAggregation with State:';
SELECT finalizeAggregation(sumSparkbarState(5, 0, 4)(number, number + 1))
FROM numbers(5);

-- Test negative range with signed integers
SELECT 'Negative range:';
SELECT sumSparkbar(5, -5, -1)(toInt64(number) - 5, 1) FROM numbers(5);

-- Test Nullable key handling
SELECT 'Nullable key:';
SELECT sumSparkbar(5, 0, 4)(
    if(number % 2 = 0, number, NULL)::Nullable(UInt64),
    1
) FROM numbers(10);

-- Test boundary values (keys at exact bucket boundaries)
SELECT 'Boundary values:';
SELECT sumSparkbar(5, 0, 4)(number, 1) FROM (
    SELECT 0 AS number UNION ALL
    SELECT 4 AS number UNION ALL
    SELECT 2 AS number
);