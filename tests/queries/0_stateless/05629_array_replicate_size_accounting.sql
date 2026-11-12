-- Preserve array-offset overhead and per-element accounting, including empty arrays and nullable dictionary values.
SET allow_suspicious_low_cardinality_types = 1;

SELECT 'UInt8', countIf(byteSize(CAST(range(number % 37) AS Array(UInt8))) != 8 + 1 * (number % 37)) FROM numbers(1000);

SELECT 'UInt16', countIf(byteSize(CAST(range(number % 37) AS Array(UInt16))) != 8 + 2 * (number % 37)) FROM numbers(1000);

SELECT 'UInt32', countIf(byteSize(CAST(range(number % 37) AS Array(UInt32))) != 8 + 4 * (number % 37)) FROM numbers(1000);

SELECT 'UInt64', countIf(byteSize(CAST(range(number % 37) AS Array(UInt64))) != 8 + 8 * (number % 37)) FROM numbers(1000);

SELECT 'UInt128', countIf(byteSize(CAST(range(number % 37) AS Array(UInt128))) != 8 + 16 * (number % 37)) FROM numbers(1000);

SELECT 'Int128', countIf(byteSize(CAST(range(number % 37) AS Array(Int128))) != 8 + 16 * (number % 37)) FROM numbers(1000);

SELECT 'UInt256', countIf(byteSize(CAST(range(number % 37) AS Array(UInt256))) != 8 + 32 * (number % 37)) FROM numbers(1000);

SELECT 'Int256', countIf(byteSize(CAST(range(number % 37) AS Array(Int256))) != 8 + 32 * (number % 37)) FROM numbers(1000);

SELECT 'Decimal32', countIf(byteSize(CAST(range(number % 37) AS Array(Decimal32(2)))) != 8 + 4 * (number % 37)) FROM numbers(1000);

SELECT 'Decimal64', countIf(byteSize(CAST(range(number % 37) AS Array(Decimal64(3)))) != 8 + 8 * (number % 37)) FROM numbers(1000);

SELECT 'Decimal128', countIf(byteSize(CAST(range(number % 37) AS Array(Decimal128(3)))) != 8 + 16 * (number % 37)) FROM numbers(1000);

SELECT 'Decimal256', countIf(byteSize(CAST(range(number % 37) AS Array(Decimal256(3)))) != 8 + 32 * (number % 37)) FROM numbers(1000);

SELECT 'Float32', countIf(byteSize(CAST(range(number % 37) AS Array(Float32))) != 8 + 4 * (number % 37)) FROM numbers(1000);

SELECT 'Float64', countIf(byteSize(CAST(range(number % 37) AS Array(Float64))) != 8 + 8 * (number % 37)) FROM numbers(1000);

SELECT 'FixedString', countIf(byteSize(arrayMap(i -> toFixedString(toString(i), 17), range(number % 37))) != 8 + 17 * (number % 37)) FROM numbers(1000);

SELECT 'Nullable(UInt64)', countIf(byteSize(arrayMap(i -> if(i % 3 = 0, NULL, toUInt64(i)), range(number % 37))) != 8 + 9 * (number % 37)) FROM numbers(1000);

SELECT 'LowCardinality(Nullable(UInt64))', countIf(byteSize(CAST(arrayMap(i -> if(i % 3 = 0, NULL, toUInt64(i)), range(number % 37)) AS Array(LowCardinality(Nullable(UInt64))))) != 8 + 9 * (number % 37)) FROM numbers(1000);

SELECT 'Tuple(UInt16, Decimal128)', countIf(byteSize(arrayMap(i -> (toUInt16(i), toDecimal128(i, 3)), range(number % 37))) != 8 + 18 * (number % 37)) FROM numbers(1000);

SELECT 'Array(UInt64)', countIf(byteSize(arrayMap(i -> CAST(range(i % 5) AS Array(UInt64)), range(number % 11))) != 8 + 8 * (number % 11) + 8 * arraySum(arrayMap(i -> i % 5, range(number % 11)))) FROM numbers(1000);

SELECT 'String', countIf(byteSize(arrayMap(i -> toString(i), range(number % 37))) != 8 + 8 * (number % 37) + arraySum(arrayMap(i -> length(toString(i)), range(number % 37)))) FROM numbers(1000);
