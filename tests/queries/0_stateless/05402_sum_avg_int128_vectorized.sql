-- `sum` and `avg` without keys add `Int128` and `UInt128` columns by summing 32-bit parts in vector lanes (`sumIntoInt128`):
-- plain, with a condition (`-If`) and skipping NULLs (`Nullable`).

SELECT 'full range';
-- Random values over all 128 bits, including the sign bit of `Int128`; 1003 rows: many vectorized steps and a scalar tail.
SELECT sum(x), sumIf(x, c), sum(nx), avg(x) FROM
(
    SELECT bitShiftLeft(toInt128(intHash64(number)), 64) + intHash64(number + 1000000) AS x, intHash32(number) % 2 = 0 AS c, if(intHash32(number) % 3 = 0, NULL, x) AS nx
    FROM numbers(1003)
);
SELECT sum(x), sumIf(x, c), sum(nx), avg(x) FROM
(
    SELECT bitShiftLeft(toUInt128(intHash64(number)), 64) + intHash64(number + 1000000) AS x, intHash32(number) % 2 = 0 AS c, if(intHash32(number) % 3 = 0, NULL, x) AS nx
    FROM numbers(1003)
);

SELECT 'short ranges';
-- A step is 4 rows: fewer rows than a step, exactly one, and one more.
WITH bitShiftLeft(toInt128(intHash64(number)), 64) + intHash64(number + 1) AS x SELECT 3, sum(x), sumIf(toUInt128(x), number % 2 = 0) FROM numbers(3);
WITH bitShiftLeft(toInt128(intHash64(number)), 64) + intHash64(number + 1) AS x SELECT 4, sum(x), sumIf(toUInt128(x), number % 2 = 0) FROM numbers(4);
WITH bitShiftLeft(toInt128(intHash64(number)), 64) + intHash64(number + 1) AS x SELECT 5, sum(x), sumIf(toUInt128(x), number % 2 = 0) FROM numbers(5);

SELECT 'wrap around';
-- As before, the sums wrap around modulo 2^128.
SELECT sum(x), sumIf(x, number % 3 != 0), sum(if(number % 3 = 0, NULL, x)) FROM (SELECT toUInt128('340282366920938463463374607431768211455') AS x, number FROM numbers(100003));
