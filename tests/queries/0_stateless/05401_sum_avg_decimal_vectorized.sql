-- `sum` and `avg` without keys add `Decimal32`, `Decimal64` and `Decimal128` columns into a `Decimal128` by summing 32-bit parts
-- in vector lanes (`sumIntoInt128`): plain, with a condition (`-If`) and skipping NULLs (`Nullable`).

SELECT 'full range';
-- Random values over the whole range of each type, so every 32-bit part takes arbitrary values, including the sign.
-- 1003 rows: many vectorized steps and a scalar tail.
SELECT sum(x), sumIf(x, c), sum(nx) FROM
(
    SELECT toDecimal32(toInt64(intHash64(number) % 1999999999) - 999999999, 0) AS x, intHash32(number) % 2 = 0 AS c, if(intHash32(number) % 3 = 0, NULL, x) AS nx
    FROM numbers(1003)
);
SELECT sum(x), sumIf(x, c), sum(nx) FROM
(
    SELECT toDecimal64(toInt64(intHash64(number) % 1999999999999999999) - 999999999999999999, 0) AS x, intHash32(number) % 2 = 0 AS c, if(intHash32(number) % 3 = 0, NULL, x) AS nx
    FROM numbers(1003)
);
SELECT sum(x), sumIf(x, c), sum(nx) FROM
(
    SELECT toDecimal128((toInt128(intHash64(number)) - 9223372036854775808) * 10000000000000000000 + intHash64(number + 1000000), 0) AS x, intHash32(number) % 2 = 0 AS c, if(intHash32(number) % 3 = 0, NULL, x) AS nx
    FROM numbers(1003)
);
-- `avg` uses the same code, also for `DateTime64` (a `Decimal64`).
SELECT avg(x), avgIf(x, c) FROM
(
    SELECT fromUnixTimestamp64Milli(toInt64(intHash64(number) % 4000000000000) - 1000000000000, 'UTC') AS x, intHash32(number) % 2 = 0 AS c
    FROM numbers(1003)
);

SELECT 'short ranges';
-- A step is 8 rows for `Decimal32` and `Decimal64` and 4 rows for `Decimal128`: fewer rows than a step, exactly one, and a few more.
SELECT 3, sum(toDecimal32(intHash32(number) % 1000000000, 0)), sumIf(toDecimal64(intHash64(number) % 1000000000000000000, 0), number % 2 = 0), sum(toDecimal128(intHash64(number), 0) * -1), sumIf(toDecimal128(intHash64(number), 0) * -1, number % 2 = 0) FROM numbers(3);
SELECT 4, sum(toDecimal32(intHash32(number) % 1000000000, 0)), sumIf(toDecimal64(intHash64(number) % 1000000000000000000, 0), number % 2 = 0), sum(toDecimal128(intHash64(number), 0) * -1), sumIf(toDecimal128(intHash64(number), 0) * -1, number % 2 = 0) FROM numbers(4);
SELECT 5, sum(toDecimal32(intHash32(number) % 1000000000, 0)), sumIf(toDecimal64(intHash64(number) % 1000000000000000000, 0), number % 2 = 0), sum(toDecimal128(intHash64(number), 0) * -1), sumIf(toDecimal128(intHash64(number), 0) * -1, number % 2 = 0) FROM numbers(5);
SELECT 7, sum(toDecimal32(intHash32(number) % 1000000000, 0)), sumIf(toDecimal64(intHash64(number) % 1000000000000000000, 0), number % 2 = 0), sum(toDecimal128(intHash64(number), 0) * -1), sumIf(toDecimal128(intHash64(number), 0) * -1, number % 2 = 0) FROM numbers(7);
SELECT 8, sum(toDecimal32(intHash32(number) % 1000000000, 0)), sumIf(toDecimal64(intHash64(number) % 1000000000000000000, 0), number % 2 = 0), sum(toDecimal128(intHash64(number), 0) * -1), sumIf(toDecimal128(intHash64(number), 0) * -1, number % 2 = 0) FROM numbers(8);
SELECT 9, sum(toDecimal32(intHash32(number) % 1000000000, 0)), sumIf(toDecimal64(intHash64(number) % 1000000000000000000, 0), number % 2 = 0), sum(toDecimal128(intHash64(number), 0) * -1), sumIf(toDecimal128(intHash64(number), 0) * -1, number % 2 = 0) FROM numbers(9);

SELECT 'extremes';
-- Every 32-bit part at its extreme, over 100003 rows, so the 64-bit lanes take large values.
SELECT sum(x), sumIf(x, number % 3 != 0), sum(if(number % 3 = 0, NULL, x)) FROM (SELECT toDecimal32(999999999, 0) AS x, number FROM numbers(100003));
SELECT sum(x), sumIf(x, number % 3 != 0), sum(if(number % 3 = 0, NULL, x)) FROM (SELECT toDecimal32(-999999999, 0) AS x, number FROM numbers(100003));
SELECT sum(x), sumIf(x, number % 3 != 0), sum(if(number % 3 = 0, NULL, x)) FROM (SELECT toDecimal64('999999999999999999', 0) AS x, number FROM numbers(100003));
SELECT sum(x), sumIf(x, number % 3 != 0), sum(if(number % 3 = 0, NULL, x)) FROM (SELECT toDecimal64('-999999999999999999', 0) AS x, number FROM numbers(100003));
SELECT sum(x), sumIf(x, number % 3 != 0), sum(if(number % 3 = 0, NULL, x)) FROM (SELECT toDecimal128(-1, 0) AS x, number FROM numbers(100003));
SELECT sum(x), sumIf(x, number % 3 != 0), sum(if(number % 3 = 0, NULL, x)) FROM (SELECT toDecimal128('-99999999999999999999999999999999999999', 0) AS x, number FROM numbers(100003));

SELECT 'Decimal128 wraps around';
-- As before, the sum wraps around modulo 2^128 instead of throwing.
SELECT sum(x), sumWithOverflow(x) FROM (SELECT toDecimal128('99999999999999999999999999999999999999', 0) AS x FROM numbers(100003));
