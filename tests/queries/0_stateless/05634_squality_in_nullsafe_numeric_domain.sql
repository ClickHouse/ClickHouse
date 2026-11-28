SET transform_null_in = 1;
SET allow_lossy_numeric_supertype = 0;

SELECT d IN (materialize(x)), d NOT IN (materialize(x))
FROM values('d Nullable(Decimal(18,0)), x Float64', (1, 1));

SELECT d IN (materialize(x)), d NOT IN (materialize(x))
FROM values('d Nullable(Int128), x Float64', (1, 1));

SELECT d IN (materialize(x)), d NOT IN (materialize(x))
FROM values('d Nullable(Int256), x UInt256', (1, 1));

SELECT d IN (materialize(x)), d NOT IN (materialize(x))
FROM values('d Nullable(Decimal(18,0)), x Float64', (NULL, 1));

SELECT toFloat64(toFloat64(toFloat64(toFloat64(toFloat64(toFloat64(toFloat64(toFloat64(x IN (materialize(x))) IN (materialize(x))) IN (materialize(x))) IN (materialize(x))) IN (materialize(x))) IN (materialize(x))) IN (materialize(x))) IN (materialize(x)))
FROM values('x Float64', 1)
SETTINGS allow_lossy_numeric_supertype = 1, max_expanded_ast_elements = 1000;
