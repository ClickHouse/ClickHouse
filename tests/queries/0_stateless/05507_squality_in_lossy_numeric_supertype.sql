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

SELECT
    toDecimal64('9007199254740993', 0) IN (x),
    toDecimal64('9007199254740993', 0) NOT IN (x),
    toDecimal64('9007199254740993', 0) IN (x, 0.),
    toDecimal64('9007199254740993', 0) NOT IN (x, 0.),
    toDecimal64('9007199254740993', 0) IN (9007199254740992., 0.)
FROM values('x Float64', 9007199254740992)
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 0;

SELECT
    toDecimal64('9007199254740993', 0) IN (x),
    toDecimal64('9007199254740993', 0) NOT IN (x),
    toDecimal64('9007199254740993', 0) IN (x, 0.),
    toDecimal64('9007199254740993', 0) NOT IN (x, 0.),
    toDecimal64('9007199254740993', 0) IN (9007199254740992., 0.)
FROM values('x Float64', 9007199254740992)
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 1;

SELECT x IN (y, 0.5), x NOT IN (y, 0.5)
FROM values('x Nullable(Int64), y Nullable(Float64)', (NULL, 0.5))
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 1;

SELECT x IN (y, 0.5), x NOT IN (y, 0.5)
FROM values('x Nullable(Int64), y Nullable(Float64)', (NULL, NULL))
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 1;

SELECT x IN (y, 0.5), x NOT IN (y, 0.5)
FROM values('x Nullable(Int64), y Nullable(Float64)', (NULL, 0.5))
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 0;

SELECT x IN (y, NULL), x NOT IN (y, NULL)
FROM values('x Float64, y Int64', (1., 1), (1.5, 1))
ORDER BY y, x
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 0;

SELECT x IN (y, NULL), x NOT IN (y, NULL)
FROM values('x Nullable(Float64), y Int64', (NULL, 1), (1., 1), (1.5, 1))
ORDER BY isNull(x), x
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 1;

SELECT x IN (toDecimal64(1, 11), y), x NOT IN (toDecimal64(1, 11), y)
FROM values('x Float32, y Int64', (1, 65))
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 0;

SELECT x IN (toDecimal64(1, 11), y), x NOT IN (toDecimal64(1, 11), y)
FROM values('x Float32, y Int64', (1, 65))
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 1;

SELECT x IN (toDecimal128(1, 23), y), x NOT IN (toDecimal128(1, 23), y)
FROM values('x Float64, y Int64', (1, 65))
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 0;

SELECT x IN (toDecimal128(1, 23), y), x NOT IN (toDecimal128(1, 23), y)
FROM values('x Float64, y Int64', (1, 65))
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 1;

SELECT x IN (d), x NOT IN (d), x IN (d, y), x NOT IN (d, y)
FROM values('x BFloat16, d Decimal(18, 0), y Int64', (1, 1, 65))
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 0;

SELECT x IN (d), x NOT IN (d), x IN (d, y), x NOT IN (d, y)
FROM values('x BFloat16, d Decimal(18, 0), y Int64', (1, 1, 65))
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 1;

SELECT x IN (d, y), x NOT IN (d, y)
FROM values('x Float64, d Decimal(18, 1), y Int64', (0.1, 0.1, 65))
SETTINGS allow_lossy_numeric_supertype = 1;

SELECT d IN (x, y), d NOT IN (x, y), d IN (1000000000000000128., 65)
FROM values('x Float64, d Decimal(38, 0), y Int64',
    (1000000000000000128., '1000000000000000100', 65),
    (1000000000000000128., '1000000000000000128', 65))
ORDER BY d
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 0;

SELECT d IN (x, y), d NOT IN (x, y), d IN (1000000000000000128., 65)
FROM values('x Float64, d Decimal(38, 0), y Int64',
    (1000000000000000128., '1000000000000000100', 65),
    (1000000000000000128., '1000000000000000128', 65))
ORDER BY d
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 1;

SELECT d IN (x, y), d NOT IN (x, y)
FROM values('x Float32, d Decimal(18, 0), y Int64', (100000008, 100000008, 65))
SETTINGS allow_lossy_numeric_supertype = 1;

SELECT x IN (d, f), x NOT IN (d, f)
FROM values('x UInt64, d Decimal(18, 0), f Float64', (1, 1, 0.5), ('18446744073709551615', 1, 0.5))
ORDER BY x
SETTINGS allow_lossy_numeric_supertype = 1;

SELECT x IN (d, f), x NOT IN (d, f)
FROM values('x UInt128, d Decimal(18, 0), f Float64', ('340282366920938463463374607431768211455', 1, 0.5))
SETTINGS allow_lossy_numeric_supertype = 1;

SELECT x IN (d, f), x NOT IN (d, f)
FROM values('x UInt256, d Decimal(18, 0), f Float64',
    ('115792089237316195423570985008687907853269984665640564039457584007913129639935', 1, 0.5))
SETTINGS allow_lossy_numeric_supertype = 1;

SELECT d IN (x, f), d NOT IN (x, f)
FROM values('x Int64, d Decimal(18, 1), f Float64', (1, 1.0, 0.5), (1, 1.5, 0.5))
ORDER BY d
SETTINGS allow_lossy_numeric_supertype = 1;

SELECT d IN (x, f), d NOT IN (x, f)
FROM values('x UInt256, d Decimal256(0), f Float64',
    ('9999999999999999999999999999999999999999999999999999999999999999999999999999',
     '9999999999999999999999999999999999999999999999999999999999999999999999999999', 0.5))
SETTINGS allow_lossy_numeric_supertype = 1;

SELECT d IN (x), d NOT IN (x), d IN (x, y), d NOT IN (x, y), d IN (100000000000000000000., 65)
FROM values('d Decimal(38, 0), x Float64, y Int64',
    ('100000000000000000001', 100000000000000000000., 65))
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 0;

SELECT d IN (x), d NOT IN (x), d IN (x, y), d NOT IN (x, y), d IN (100000000000000000000., 65)
FROM values('d Decimal(38, 0), x Float64, y Int64',
    ('100000000000000000001', 100000000000000000000., 65))
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 1;

SELECT d IN (x, y), d NOT IN (x, y)
FROM values('d Decimal(18, 0), x Float64, y Int64',
    (0, 'nan', 65), (1, 'inf', 65), (2, '-inf', 65), (3, '1e100', 65))
ORDER BY d
SETTINGS allow_lossy_numeric_supertype = 1;

SELECT d IN (materialize(x)), d NOT IN (materialize(x))
FROM values('d Decimal(18, 0), x Float64', ('9007199254740993', 9007199254740992.))
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 0;

SELECT d IN (materialize(x)), d NOT IN (materialize(x))
FROM values('d Decimal(18, 0), x Float64', ('9007199254740993', 9007199254740992.))
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 1;

SELECT d IN (x, y), d NOT IN (x, y)
FROM values('d Decimal(76, 0), x Float64, y Int64',
    (1, 1e60, 65),
    ('999999999999999949387135297074018866963645011013410073083904', 1e60, 65))
ORDER BY d
SETTINGS allow_lossy_numeric_supertype = 1;


-- Nested numeric comparisons remain evaluable with the distributed-compatible rewrite.
SELECT d IN (toFloat64(d IN (toFloat64(d IN (toFloat64(d IN (x)))))))
FROM values('d Decimal(18,0), x Float64', (1,1))
SETTINGS max_expanded_ast_elements = 1000;

SELECT toFloat64(d IN (toFloat64(d IN (toFloat64(d IN (toFloat64(d IN (toFloat64(d IN (x))))))))))
FROM values('d Decimal(18,0), x Float64', (1,1))
SETTINGS allow_lossy_numeric_supertype = 1, transform_null_in = 1, max_expanded_ast_elements = 100000;

-- Generated helpers bind to built-ins rather than scoped lambdas.
WITH (v -> v) AS trunc
SELECT d IN (materialize(x)), d NOT IN (materialize(x))
FROM values('d Decimal(18,1), x Int64', (1.5,1));

-- Exercise the bounded comparison over a non-trivial vector.
SELECT count()
FROM numbers(1000000)
WHERE toDecimal64(number, 0) IN (materialize(toFloat64(number)))
SETTINGS allow_lossy_numeric_supertype = 1;

-- Generated membership keeps its exact comparison separate from ordinary equality.
SELECT d = materialize(x), d IN (materialize(x)), d NOT IN (materialize(x))
FROM values('d Decimal(18, 0), x Float64', ('9007199254740993', 9007199254740992.))
SETTINGS allow_lossy_numeric_supertype = 1;

SELECT d IN (materialize(x)), d = materialize(x), d NOT IN (materialize(x))
FROM values('d Decimal(18, 0), x Float64', ('9007199254740993', 9007199254740992.))
SETTINGS allow_lossy_numeric_supertype = 1;

SELECT d = x, d != x, isNotDistinctFrom(d, x), isDistinctFrom(d, x)
FROM values('d Decimal(18, 0), x Float64', ('9007199254740993', 9007199254740992.))
SETTINGS allow_lossy_numeric_supertype = 1;
