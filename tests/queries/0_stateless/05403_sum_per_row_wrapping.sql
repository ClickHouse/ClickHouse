-- The per-row paths of `sum` (`GROUP BY`, window functions, merging states) add with wrap-around: `Decimal` via its native
-- value and 128-bit integers as `unsigned __int128`.

SELECT 'GROUP BY';
-- Values at the limits of each type, so the additions carry into the high words and wrap around.
SELECT k, sum(d32), sum(d64), sum(d128), sum(d256), sum(i128), sum(u128)
FROM
(
    SELECT
        number % 3 AS k,
        toDecimal32(if(number % 2, 999999999, -999999998), 0) AS d32,
        toDecimal64(if(number % 2, '999999999999999999', '-999999999999999998'), 0) AS d64,
        toDecimal128(if(number % 2, '99999999999999999999999999999999999999', '-99999999999999999999999999999999999998'), 0) AS d128,
        toDecimal256(if(number % 2, '9999999999999999999999999999999999999999999999999999999999999999999999999999', '-9999999999999999999999999999999999999999999999999999999999999999999999999998'), 0) AS d256,
        toInt128(if(number % 2, '170141183460469231731687303715884105727', '-170141183460469231731687303715884105727')) AS i128,
        bitXor(toUInt128('340282366920938463463374607431768211455'), number) AS u128
    FROM numbers(10001)
)
GROUP BY k
ORDER BY k;

SELECT 'sumWithOverflow wraps around in the argument type';
SELECT k, sumWithOverflow(d32), sumWithOverflow(d64), sumWithOverflow(d128), sumWithOverflow(i128)
FROM
(
    SELECT number % 2 AS k, toDecimal32(999999999, 0) AS d32, toDecimal64('999999999999999999', 0) AS d64,
        toDecimal128('99999999999999999999999999999999999999', 0) AS d128, toInt128('170141183460469231731687303715884105727') AS i128
    FROM numbers(1001)
)
GROUP BY k
ORDER BY k;

SELECT 'window';
SELECT n, sum(d128) OVER w, sum(i128) OVER w, sumWithOverflow(d64) OVER w
FROM
(
    SELECT number AS n, toDecimal128('99999999999999999999999999999999999999', 0) * if(number % 3 = 0, -1, 1) AS d128,
        toInt128('170141183460469231731687303715884105727') AS i128, toDecimal64('999999999999999999', 0) AS d64
    FROM numbers(6)
)
WINDOW w AS (ORDER BY n ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
ORDER BY n;

SELECT 'merge';
SELECT sumMerge(s) FROM (SELECT sumState(toDecimal128('99999999999999999999999999999999999999', 0)) AS s FROM numbers(1000) GROUP BY number % 7);
SELECT sumMerge(s) FROM (SELECT sumState(toUInt128('340282366920938463463374607431768211455')) AS s FROM numbers(1000) GROUP BY number % 7);
