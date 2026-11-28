WITH
    number % 4 AS selector,
    toUInt8(number) AS value,
    [[], [value], [value, value]] AS arrays,
    multiIf(selector = 1, [value], selector = 2, [value, value], []) AS expected
SELECT countIf(formatRowNoNewline('Values', arrays[selector + 1]) != formatRowNoNewline('Values', expected))
FROM numbers(40);

WITH
    number % 4 AS selector,
    toInt64(number) - 20 AS value,
    [[], [value], [value, value]] AS arrays,
    multiIf(selector = 1, [value], selector = 2, [value, value], []) AS expected
SELECT countIf(formatRowNoNewline('Values', arrays[selector + 1]) != formatRowNoNewline('Values', expected))
FROM numbers(40);

WITH
    number % 4 AS selector,
    toUInt128(number) AS value,
    [[], [value], [value, value]] AS arrays,
    multiIf(selector = 1, [value], selector = 2, [value, value], []) AS expected
SELECT countIf(formatRowNoNewline('Values', arrays[selector + 1]) != formatRowNoNewline('Values', expected))
FROM numbers(40);

WITH
    number % 4 AS selector,
    if(number % 3 = 0, NULL, toInt64(number)) AS value,
    [[], [value], [value, value]] AS arrays,
    multiIf(selector = 1, [value], selector = 2, [value, value], []) AS expected
SELECT countIf(formatRowNoNewline('Values', arrays[selector + 1]) != formatRowNoNewline('Values', expected))
FROM numbers(40);

WITH
    number % 4 AS selector,
    concat(toString(number), '\0', repeat('x', 1000)) AS value,
    [[], [value], [value, value]] AS arrays,
    multiIf(selector = 1, [value], selector = 2, [value, value], []) AS expected
SELECT countIf(formatRowNoNewline('Values', arrays[selector + 1]) != formatRowNoNewline('Values', expected))
FROM numbers(40);

WITH
    number % 4 AS selector,
    toFixedString(toString(number % 10), 8) AS value,
    [[], [value], [value, value]] AS arrays,
    multiIf(selector = 1, [value], selector = 2, [value, value], []) AS expected
SELECT countIf(formatRowNoNewline('Values', arrays[selector + 1]) != formatRowNoNewline('Values', expected))
FROM numbers(40);

WITH
    number % 4 AS selector,
    tuple(number, toString(number)) AS value,
    [[], [value], [value, value]] AS arrays,
    multiIf(selector = 1, [value], selector = 2, [value, value], []) AS expected
SELECT countIf(formatRowNoNewline('Values', arrays[selector + 1]) != formatRowNoNewline('Values', expected))
FROM numbers(40);

WITH
    number % 4 AS selector,
    [number, number + 1] AS value,
    [[], [value], [value, value]] AS arrays,
    multiIf(selector = 1, [value], selector = 2, [value, value], []) AS expected
SELECT countIf(formatRowNoNewline('Values', arrays[selector + 1]) != formatRowNoNewline('Values', expected))
FROM numbers(40);

WITH
    number % 4 AS selector,
    map('key', number) AS value,
    [[], [value], [value, value]] AS arrays,
    multiIf(selector = 1, [value], selector = 2, [value, value], []) AS expected
SELECT countIf(formatRowNoNewline('Values', arrays[selector + 1]) != formatRowNoNewline('Values', expected))
FROM numbers(40);

WITH [[reinterpretAsFloat64(toUInt64(9223372036854775808))], [reinterpretAsFloat64(toUInt64(9218868437227405313))], [reinterpretAsFloat64(toUInt64(9221120237041095220))]] AS arrays
SELECT arrayMap(x -> reinterpretAsUInt64(x), arrays[number + 1])
FROM numbers(3);

WITH [[toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935')]] AS arrays
SELECT arrays[number + 1]
FROM numbers(2);
