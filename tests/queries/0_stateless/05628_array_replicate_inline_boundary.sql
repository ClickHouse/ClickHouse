-- Exercise empty rows, zero repeats and numeric copy sizes on both sides of the 64- and 128-byte boundaries.
-- Validate each result element from its row and element index, independently of the copied source array.

SELECT 'UInt8', count(), countIf(length(a) != number % 132 OR arrayExists((x, i) -> x != (toUInt8(number * 1000 + i - 1)), a, arrayEnumerate(a)))
FROM
(
    SELECT number, arrayJoin(arrayWithConstant(number % 15, arrayMap(i -> toUInt8(number * 1000 + i), range(number % 132)))) AS a
    FROM numbers(512)
);

SELECT 'UInt16', count(), countIf(length(a) != number % 68 OR arrayExists((x, i) -> x != (toUInt16(number * 1000 + i - 1)), a, arrayEnumerate(a)))
FROM
(
    SELECT number, arrayJoin(arrayWithConstant(number % 15, arrayMap(i -> toUInt16(number * 1000 + i), range(number % 68)))) AS a
    FROM numbers(512)
);

SELECT 'UInt32', count(), countIf(length(a) != number % 36 OR arrayExists((x, i) -> x != (toUInt32(number * 1000 + i - 1)), a, arrayEnumerate(a)))
FROM
(
    SELECT number, arrayJoin(arrayWithConstant(number % 15, arrayMap(i -> toUInt32(number * 1000 + i), range(number % 36)))) AS a
    FROM numbers(512)
);

SELECT 'UInt64', count(), countIf(length(a) != number % 20 OR arrayExists((x, i) -> x != (toUInt64(number * 1000 + i - 1)), a, arrayEnumerate(a)))
FROM
(
    SELECT number, arrayJoin(arrayWithConstant(number % 15, arrayMap(i -> toUInt64(number * 1000 + i), range(number % 20)))) AS a
    FROM numbers(512)
);

SELECT 'UInt128', count(), countIf(length(a) != number % 12 OR arrayExists((x, i) -> x != (bitShiftLeft(toUInt128(1), 100) + toUInt128(number * 1000 + i - 1)), a, arrayEnumerate(a)))
FROM
(
    SELECT number, arrayJoin(arrayWithConstant(number % 15, arrayMap(i -> bitShiftLeft(toUInt128(1), 100) + toUInt128(number * 1000 + i), range(number % 12)))) AS a
    FROM numbers(512)
);

SELECT 'Int128', count(), countIf(length(a) != number % 12 OR arrayExists((x, i) -> x != (-bitShiftLeft(toInt128(1), 100) + toInt128(number * 1000 + i - 1)), a, arrayEnumerate(a)))
FROM
(
    SELECT number, arrayJoin(arrayWithConstant(number % 15, arrayMap(i -> -bitShiftLeft(toInt128(1), 100) + toInt128(number * 1000 + i), range(number % 12)))) AS a
    FROM numbers(512)
);

SELECT 'UInt256', count(), countIf(length(a) != number % 8 OR arrayExists((x, i) -> x != (bitShiftLeft(toUInt256(1), 200) + toUInt256(number * 1000 + i - 1)), a, arrayEnumerate(a)))
FROM
(
    SELECT number, arrayJoin(arrayWithConstant(number % 15, arrayMap(i -> bitShiftLeft(toUInt256(1), 200) + toUInt256(number * 1000 + i), range(number % 8)))) AS a
    FROM numbers(512)
);

SELECT 'Int256', count(), countIf(length(a) != number % 8 OR arrayExists((x, i) -> x != (-bitShiftLeft(toInt256(1), 200) + toInt256(number * 1000 + i - 1)), a, arrayEnumerate(a)))
FROM
(
    SELECT number, arrayJoin(arrayWithConstant(number % 15, arrayMap(i -> -bitShiftLeft(toInt256(1), 200) + toInt256(number * 1000 + i), range(number % 8)))) AS a
    FROM numbers(512)
);

SELECT 'Decimal32', count(), countIf(length(a) != number % 36 OR arrayExists((x, i) -> x != (toDecimal32(number * 1000 + i - 1, 2)), a, arrayEnumerate(a)))
FROM
(
    SELECT number, arrayJoin(arrayWithConstant(number % 15, arrayMap(i -> toDecimal32(number * 1000 + i, 2), range(number % 36)))) AS a
    FROM numbers(512)
);

SELECT 'Decimal64', count(), countIf(length(a) != number % 20 OR arrayExists((x, i) -> x != (toDecimal64(number * 1000 + i - 1, 3)), a, arrayEnumerate(a)))
FROM
(
    SELECT number, arrayJoin(arrayWithConstant(number % 15, arrayMap(i -> toDecimal64(number * 1000 + i, 3), range(number % 20)))) AS a
    FROM numbers(512)
);

SELECT 'Decimal128', count(), countIf(length(a) != number % 12 OR arrayExists((x, i) -> x != (toDecimal128('123456789012345678901234567890.123', 3) + toDecimal128(number * 1000 + i - 1, 3)), a, arrayEnumerate(a)))
FROM
(
    SELECT number, arrayJoin(arrayWithConstant(number % 15, arrayMap(i -> toDecimal128('123456789012345678901234567890.123', 3) + toDecimal128(number * 1000 + i, 3), range(number % 12)))) AS a
    FROM numbers(512)
);

SELECT 'Decimal256', count(), countIf(length(a) != number % 8 OR arrayExists((x, i) -> x != (toDecimal256('12345678901234567890123456789012345678901234567890.123', 3) + toDecimal256(number * 1000 + i - 1, 3)), a, arrayEnumerate(a)))
FROM
(
    SELECT number, arrayJoin(arrayWithConstant(number % 15, arrayMap(i -> toDecimal256('12345678901234567890123456789012345678901234567890.123', 3) + toDecimal256(number * 1000 + i, 3), range(number % 8)))) AS a
    FROM numbers(512)
);

SELECT 'Float32', count(), countIf(length(a) != number % 36 OR arrayExists((x, i) -> x != (toFloat32(number * 1000 + i - 1)), a, arrayEnumerate(a)))
FROM
(
    SELECT number, arrayJoin(arrayWithConstant(number % 15, arrayMap(i -> toFloat32(number * 1000 + i), range(number % 36)))) AS a
    FROM numbers(512)
);

SELECT 'Float64', count(), countIf(length(a) != number % 20 OR arrayExists((x, i) -> x != (toFloat64(number * 1000 + i - 1)), a, arrayEnumerate(a)))
FROM
(
    SELECT number, arrayJoin(arrayWithConstant(number % 15, arrayMap(i -> toFloat64(number * 1000 + i), range(number % 20)))) AS a
    FROM numbers(512)
);
