WITH
    number % 4 AS selector,
    toDecimal64(toInt64(number) - 20, 4) AS value,
    [[], [value], [value, value]] AS arrays,
    multiIf(selector = 1, [value], selector = 2, [value, value], []) AS expected
SELECT countIf(formatRowNoNewline('Values', arrays[selector + 1]) != formatRowNoNewline('Values', expected))
FROM numbers(40);

WITH
    number % 4 AS selector,
    toDecimal256('1234567890123456789012345678901234567890.123456', 6) + toDecimal256(number, 6) AS value,
    [[], [value], [value, value]] AS arrays,
    multiIf(selector = 1, [value], selector = 2, [value, value], []) AS expected
SELECT countIf(formatRowNoNewline('Values', arrays[selector + 1]) != formatRowNoNewline('Values', expected))
FROM numbers(40);

WITH
    number % 4 AS selector,
    if(number % 3 = 0, NULL, toDecimal64(toInt64(number) - 20, 4)) AS value,
    [[], [value], [value, value]] AS arrays,
    multiIf(selector = 1, [value], selector = 2, [value, value], []) AS expected
SELECT countIf(formatRowNoNewline('Values', arrays[selector + 1]) != formatRowNoNewline('Values', expected))
FROM numbers(40);

WITH
    number % 4 AS selector,
    toString(number) AS value,
    CAST([[], [value], [value, value]], 'Array(Array(LowCardinality(String)))') AS arrays,
    multiIf(selector = 1, [value], selector = 2, [value, value], []) AS expected
SELECT countIf(formatRowNoNewline('Values', arrays[selector + 1]) != formatRowNoNewline('Values', expected))
FROM numbers(40);
