-- Two-argument `least`/`greatest`: a NaN argument loses against any number, with or without JIT compilation.
-- https://github.com/ClickHouse/ClickHouse/issues/120728

-- Float64
SELECT 'Float64 JIT off', id, least(-x, y), least(y, -x), greatest(-x, y), greatest(y, -x)
FROM values('id UInt8, x Float64, y Float64', (1, -1, nan), (2, nan, 1), (3, nan, nan), (4, -inf, nan), (5, 1, 2))
ORDER BY id
SETTINGS compile_expressions = 0;

SELECT 'Float64 JIT on', id, least(-x, y), least(y, -x), greatest(-x, y), greatest(y, -x)
FROM values('id UInt8, x Float64, y Float64', (1, -1, nan), (2, nan, 1), (3, nan, nan), (4, -inf, nan), (5, 1, 2))
ORDER BY id
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

-- Float32
SELECT 'Float32 JIT off', id, least(-x, y), least(y, -x), greatest(-x, y), greatest(y, -x)
FROM values('id UInt8, x Float32, y Float32', (1, -1, nan), (2, nan, 1), (3, nan, nan), (4, -inf, nan), (5, 1, 2))
ORDER BY id
SETTINGS compile_expressions = 0;

SELECT 'Float32 JIT on', id, least(-x, y), least(y, -x), greatest(-x, y), greatest(y, -x)
FROM values('id UInt8, x Float32, y Float32', (1, -1, nan), (2, nan, 1), (3, nan, nan), (4, -inf, nan), (5, 1, 2))
ORDER BY id
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

-- Int32 promoted to Float64
SELECT 'promoted JIT off', least(-i, y), least(y, -i), greatest(-i, y), greatest(y, -i)
FROM values('i Int32, y Float64', (-1, nan))
SETTINGS compile_expressions = 0;

SELECT 'promoted JIT on', least(-i, y), least(y, -i), greatest(-i, y), greatest(y, -i)
FROM values('i Int32, y Float64', (-1, nan))
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

-- Equal operands of different sign: the right operand is returned
SELECT 'signed zero JIT off', 1 / least(-x, y), 1 / least(y, -x), 1 / greatest(-x, y), 1 / greatest(y, -x)
FROM values('x Float64, y Float64', (0, 0))
SETTINGS compile_expressions = 0;

SELECT 'signed zero JIT on', 1 / least(-x, y), 1 / least(y, -x), 1 / greatest(-x, y), 1 / greatest(y, -x)
FROM values('x Float64, y Float64', (0, 0))
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

-- Constant folding
SELECT 'constant', least(1., nan), least(nan, 1.), greatest(1., nan), greatest(nan, 1.);

-- BFloat16 (interpreted only)
SELECT 'BFloat16',
    least(materialize(toBFloat16(1)), materialize(toBFloat16(nan))),
    least(materialize(toBFloat16(nan)), materialize(toBFloat16(1))),
    greatest(materialize(toBFloat16(1)), materialize(toBFloat16(nan))),
    greatest(materialize(toBFloat16(nan)), materialize(toBFloat16(1)));

-- Nullable arguments with the legacy NULL behavior
SELECT 'legacy null',
    least(materialize(toNullable(1.)), materialize(toNullable(nan))),
    greatest(materialize(toNullable(1.)), materialize(toNullable(nan)))
SETTINGS least_greatest_legacy_null_behavior = 1, compile_expressions = 0;
