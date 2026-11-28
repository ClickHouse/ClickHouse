WITH
    [[], [1], [2, 3]] AS arrays,
    [0, 1, 2, 3, 4, -1, -2, -3, -4, toInt64('-9223372036854775808'), toInt64('9223372036854775807'), 2, 2][number % 13 + 1] AS idx,
    multiIf(idx IN (2, -2), arrays[2], idx IN (3, -1), arrays[3], arrays[1]) AS expected
SELECT
    countIf(formatRowNoNewline('Values', arrayElement(arrays, idx)) != formatRowNoNewline('Values', expected)),
    countIf(formatRowNoNewline('Values', arrayElementOrNull(arrays, idx)) != formatRowNoNewline('Values', expected))
FROM numbers(10000)
SETTINGS max_threads = 1, max_block_size = 256;

WITH
    CAST([[], [1], [2, 3]], 'Array(Array(UInt64))') AS arrays,
    [0, 1, 2, 3, 4, -1, -2, -3, -4, toInt64('-9223372036854775808'), toInt64('9223372036854775807'), 2, 2][number % 13 + 1] AS idx,
    multiIf(idx IN (2, -2), arrays[2], idx IN (3, -1), arrays[3], arrays[1]) AS expected
SELECT
    countIf(formatRowNoNewline('Values', arrayElement(arrays, idx)) != formatRowNoNewline('Values', expected)),
    countIf(formatRowNoNewline('Values', arrayElementOrNull(arrays, idx)) != formatRowNoNewline('Values', expected))
FROM numbers(10000)
SETTINGS max_threads = 1, max_block_size = 256;

WITH
    [[], [toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935')], [toUInt256(2), toUInt256(3)]] AS arrays,
    [0, 1, 2, 3, 4, -1, -2, -3, -4, toInt64('-9223372036854775808'), toInt64('9223372036854775807'), 2, 2][number % 13 + 1] AS idx,
    multiIf(idx IN (2, -2), arrays[2], idx IN (3, -1), arrays[3], arrays[1]) AS expected
SELECT
    countIf(formatRowNoNewline('Values', arrayElement(arrays, idx)) != formatRowNoNewline('Values', expected)),
    countIf(formatRowNoNewline('Values', arrayElementOrNull(arrays, idx)) != formatRowNoNewline('Values', expected))
FROM numbers(10000)
SETTINGS max_threads = 1, max_block_size = 256;

WITH
    CAST([[], [1.25], [2.5, 3.75]], 'Array(Array(Decimal64(4)))') AS arrays,
    [0, 1, 2, 3, 4, -1, -2, -3, -4, toInt64('-9223372036854775808'), toInt64('9223372036854775807'), 2, 2][number % 13 + 1] AS idx,
    multiIf(idx IN (2, -2), arrays[2], idx IN (3, -1), arrays[3], arrays[1]) AS expected
SELECT
    countIf(formatRowNoNewline('Values', arrayElement(arrays, idx)) != formatRowNoNewline('Values', expected)),
    countIf(formatRowNoNewline('Values', arrayElementOrNull(arrays, idx)) != formatRowNoNewline('Values', expected))
FROM numbers(10000)
SETTINGS max_threads = 1, max_block_size = 256;

WITH
    [[], [toDecimal256('1234567890123456789012345678901234567890.123456', 6)], [toDecimal256(2, 6), toDecimal256(3, 6)]] AS arrays,
    [0, 1, 2, 3, 4, -1, -2, -3, -4, toInt64('-9223372036854775808'), toInt64('9223372036854775807'), 2, 2][number % 13 + 1] AS idx,
    multiIf(idx IN (2, -2), arrays[2], idx IN (3, -1), arrays[3], arrays[1]) AS expected
SELECT
    countIf(formatRowNoNewline('Values', arrayElement(arrays, idx)) != formatRowNoNewline('Values', expected)),
    countIf(formatRowNoNewline('Values', arrayElementOrNull(arrays, idx)) != formatRowNoNewline('Values', expected))
FROM numbers(10000)
SETTINGS max_threads = 1, max_block_size = 256;

WITH
    CAST([[], ['a'], ['b', 'c']], 'Array(Array(FixedString(8)))') AS arrays,
    [0, 1, 2, 3, 4, -1, -2, -3, -4, toInt64('-9223372036854775808'), toInt64('9223372036854775807'), 2, 2][number % 13 + 1] AS idx,
    multiIf(idx IN (2, -2), arrays[2], idx IN (3, -1), arrays[3], arrays[1]) AS expected
SELECT
    countIf(formatRowNoNewline('Values', arrayElement(arrays, idx)) != formatRowNoNewline('Values', expected)),
    countIf(formatRowNoNewline('Values', arrayElementOrNull(arrays, idx)) != formatRowNoNewline('Values', expected))
FROM numbers(10000)
SETTINGS max_threads = 1, max_block_size = 256;

WITH
    [[], ['a\0b'], [repeat('x', 1000), 'c']] AS arrays,
    [0, 1, 2, 3, 4, -1, -2, -3, -4, toInt64('-9223372036854775808'), toInt64('9223372036854775807'), 2, 2][number % 13 + 1] AS idx,
    multiIf(idx IN (2, -2), arrays[2], idx IN (3, -1), arrays[3], arrays[1]) AS expected
SELECT
    countIf(formatRowNoNewline('Values', arrayElement(arrays, idx)) != formatRowNoNewline('Values', expected)),
    countIf(formatRowNoNewline('Values', arrayElementOrNull(arrays, idx)) != formatRowNoNewline('Values', expected))
FROM numbers(10000)
SETTINGS max_threads = 1, max_block_size = 256;

WITH
    CAST([[], [NULL], ['b\0c', NULL]], 'Array(Array(Nullable(String)))') AS arrays,
    [0, 1, 2, 3, 4, -1, -2, -3, -4, toInt64('-9223372036854775808'), toInt64('9223372036854775807'), 2, 2][number % 13 + 1] AS idx,
    multiIf(idx IN (2, -2), arrays[2], idx IN (3, -1), arrays[3], arrays[1]) AS expected
SELECT
    countIf(formatRowNoNewline('Values', arrayElement(arrays, idx)) != formatRowNoNewline('Values', expected)),
    countIf(formatRowNoNewline('Values', arrayElementOrNull(arrays, idx)) != formatRowNoNewline('Values', expected))
FROM numbers(10000)
SETTINGS max_threads = 1, max_block_size = 256;

WITH
    CAST([[], ['a'], ['b', 'c']], 'Array(Array(LowCardinality(String)))') AS arrays,
    [0, 1, 2, 3, 4, -1, -2, -3, -4, toInt64('-9223372036854775808'), toInt64('9223372036854775807'), 2, 2][number % 13 + 1] AS idx,
    multiIf(idx IN (2, -2), arrays[2], idx IN (3, -1), arrays[3], arrays[1]) AS expected
SELECT
    countIf(formatRowNoNewline('Values', arrayElement(arrays, idx)) != formatRowNoNewline('Values', expected)),
    countIf(formatRowNoNewline('Values', arrayElementOrNull(arrays, idx)) != formatRowNoNewline('Values', expected))
FROM numbers(10000)
SETTINGS max_threads = 1, max_block_size = 256;

WITH
    [[], [tuple(1, 'a')], [tuple(2, 'b'), tuple(3, 'c')]] AS arrays,
    [0, 1, 2, 3, 4, -1, -2, -3, -4, toInt64('-9223372036854775808'), toInt64('9223372036854775807'), 2, 2][number % 13 + 1] AS idx,
    multiIf(idx IN (2, -2), arrays[2], idx IN (3, -1), arrays[3], arrays[1]) AS expected
SELECT
    countIf(formatRowNoNewline('Values', arrayElement(arrays, idx)) != formatRowNoNewline('Values', expected)),
    countIf(formatRowNoNewline('Values', arrayElementOrNull(arrays, idx)) != formatRowNoNewline('Values', expected))
FROM numbers(10000)
SETTINGS max_threads = 1, max_block_size = 256;

WITH
    [[], [[1]], [[2], []]] AS arrays,
    [0, 1, 2, 3, 4, -1, -2, -3, -4, toInt64('-9223372036854775808'), toInt64('9223372036854775807'), 2, 2][number % 13 + 1] AS idx,
    multiIf(idx IN (2, -2), arrays[2], idx IN (3, -1), arrays[3], arrays[1]) AS expected
SELECT
    countIf(formatRowNoNewline('Values', arrayElement(arrays, idx)) != formatRowNoNewline('Values', expected)),
    countIf(formatRowNoNewline('Values', arrayElementOrNull(arrays, idx)) != formatRowNoNewline('Values', expected))
FROM numbers(10000)
SETTINGS max_threads = 1, max_block_size = 256;
