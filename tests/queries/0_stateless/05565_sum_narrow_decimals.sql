-- `sum`, `avg` and `sumWithOverflow` over `Decimal32` and `Decimal64` take a vectorized path when a range of rows is
-- added to one state. `Decimal128` arguments never take it, so they give the row-by-row result to compare with.

DROP TABLE IF EXISTS t_narrow_decimals;

CREATE TABLE t_narrow_decimals
(
    k UInt64,
    c UInt8,
    nc Nullable(UInt8),
    d32 Decimal32(2),
    d64 Decimal64(3),
    n32 Nullable(Decimal32(2)),
    n64 Nullable(Decimal64(3)),
    w32 Decimal32(2),
    w64 Decimal64(3)
) ENGINE = Memory;

-- `c` takes values 0..3, so conditions are not only 0 and 1. `d64` has mixed signs and uses all 64 bits, so its
-- high and low 32-bit halves are both non-trivial. `w32` and `w64` are large enough for `sumWithOverflow` to wrap.
INSERT INTO t_narrow_decimals SELECT
    number,
    number % 4,
    if(number % 7 = 0, NULL, number % 3),
    toDecimal32((toInt64(number) - 500) * 1999999 / 100, 2),
    toDecimal64((toInt64(number) - 500) * 1234567890123, 3),
    if(number % 5 = 0, NULL, toDecimal32((toInt64(number) - 500) * 1999999 / 100, 2)),
    if(number % 5 = 0, NULL, toDecimal64((toInt64(number) - 500) * 1234567890123, 3)),
    toDecimal32(9999999 - number % 13, 2),
    toDecimal64(999999999999999 - number, 3)
FROM numbers(1000);

SELECT 'widening sums, whole column';
SELECT
    sum(d32) = sum(d32::Decimal128(2)),
    sum(d64) = sum(d64::Decimal128(3)),
    avg(d32) = avg(d32::Decimal128(2)),
    avg(d64) = avg(d64::Decimal128(3))
FROM t_narrow_decimals;

SELECT 'widening sums, -If';
SELECT
    sumIf(d32, c) = sumIf(d32::Decimal128(2), c),
    sumIf(d64, c) = sumIf(d64::Decimal128(3), c),
    avgIf(d32, c) = avgIf(d32::Decimal128(2), c),
    avgIf(d64, c) = avgIf(d64::Decimal128(3), c)
FROM t_narrow_decimals;

SELECT 'widening sums, Nullable';
SELECT
    sum(n32) = sum(n32::Nullable(Decimal128(2))),
    sum(n64) = sum(n64::Nullable(Decimal128(3))),
    avg(n32) = avg(n32::Nullable(Decimal128(2))),
    avg(n64) = avg(n64::Nullable(Decimal128(3)))
FROM t_narrow_decimals;

SELECT 'widening sums, Nullable and -If';
SELECT
    sumIf(n32, c) = sumIf(n32::Nullable(Decimal128(2)), c),
    sumIf(n64, c) = sumIf(n64::Nullable(Decimal128(3)), c),
    avgIf(n64, c) = avgIf(n64::Nullable(Decimal128(3)), c),
    sumIf(d64, nc) = sumIf(d64::Decimal128(3), nc),
    sumIf(n64, nc) = sumIf(n64::Nullable(Decimal128(3)), nc)
FROM t_narrow_decimals;

SELECT 'all rows excluded';
SELECT sumIf(d32, 0), sumIf(d64, 0), avgIf(d64, 0), sumWithOverflowIf(d32, 0), sumWithOverflowIf(d64, 0)
FROM t_narrow_decimals;

SELECT 'wrapping sums';
SELECT
    sumWithOverflow(w32),
    sumWithOverflow(w64),
    sumWithOverflowIf(w32, c),
    sumWithOverflowIf(w64, c),
    sumWithOverflow(-w64),
    sumWithOverflow(d64),
    sumWithOverflowIf(w64, nc),
    sumWithOverflow(if(k % 5 = 0, NULL, w64))
FROM t_narrow_decimals;

SELECT 'window frames with a nonzero first row';
SELECT count()
FROM
(
    SELECT
        sumIf(d64, c) OVER w AS a,
        sumIf(d64::Decimal128(3), c) OVER w AS b,
        sum(n32) OVER w AS x,
        sum(n32::Nullable(Decimal128(2))) OVER w AS y,
        sumWithOverflow(w64) OVER w AS p,
        sumWithOverflow(w64::Decimal128(3)) OVER w AS q
    FROM t_narrow_decimals
    WINDOW w AS (ORDER BY k ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
WHERE a != b OR x != y OR p != q;

-- A block whose rows all have the same key is added to one state in a single batch.
SELECT 'GROUP BY with one key per block';
SELECT count()
FROM
(
    SELECT
        intDiv(number, 50) AS g,
        sumIf(d64, c) AS a,
        sumIf(d64::Decimal128(3), c) AS b,
        sum(n32) AS x,
        sum(n32::Nullable(Decimal128(2))) AS y
    FROM
    (
        SELECT
            number,
            number % 4 AS c,
            toDecimal64((toInt64(number) - 500) * 1234567890123, 3) AS d64,
            if(number % 5 = 0, NULL, toDecimal32((toInt64(number) - 500) * 1999999 / 100, 2)) AS n32
        FROM numbers(1000)
    )
    GROUP BY g
)
WHERE a != b OR x != y
SETTINGS max_block_size = 50, max_threads = 1, compile_aggregate_expressions = 0;

DROP TABLE t_narrow_decimals;
