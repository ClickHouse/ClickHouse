SELECT
    x,
    toInt64(0) IN (toInt64(65), x),
    toInt64(0) NOT IN (toInt64(65), x)
FROM values('x Float64', (0.5), (1.5), (-0.5))
ORDER BY x
SETTINGS allow_lossy_numeric_supertype = 1;

SELECT
    x IN (x, 0.5),
    x NOT IN (x, 0.5)
FROM values('x Int64', 9007199254740993)
SETTINGS allow_lossy_numeric_supertype = 1;

SELECT
    toInt64(9007199254740992) IN (x, 0.5),
    toInt64(9007199254740992) = x
FROM values('x Int64', 9007199254740993)
SETTINGS allow_lossy_numeric_supertype = 1;

SELECT toDecimal64(1, 2) IN (toDecimal64(65, 2), x)
FROM values('x Float64', 1.)
SETTINGS allow_lossy_numeric_supertype = 1;

SELECT toDecimal64(1, 2) IN (x)
FROM values('x Float64', 1.)
SETTINGS allow_lossy_numeric_supertype = 1;

SELECT toInt128(1) IN (toInt128(65), x)
FROM values('x Float64', 1.)
SETTINGS allow_lossy_numeric_supertype = 1;
