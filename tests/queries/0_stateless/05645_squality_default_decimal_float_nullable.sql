SET allow_lossy_numeric_supertype = 0;

SELECT d IN (materialize(x)), d NOT IN (materialize(x)),
       assumeNotNull(d) IN (materialize(x)), isNotDistinctFrom(d, x)
FROM values('d Nullable(Decimal(9,2)), x Float64', ('0.29', 0.29), ('0.57', 0.57))
ORDER BY d
SETTINGS transform_null_in = 0;

SELECT d IN (materialize(x)), d NOT IN (materialize(x)),
       assumeNotNull(d) IN (materialize(x)), isNotDistinctFrom(d, x)
FROM values('d Nullable(Decimal(9,2)), x Float64', ('0.29', 0.29), ('0.57', 0.57))
ORDER BY d
SETTINGS transform_null_in = 1;

SELECT count()
FROM values('d Nullable(Decimal(9,2)), x Float64', ('0.29', 0.29), ('0.57', 0.57))
WHERE d IN (materialize(x))
SETTINGS transform_null_in = 1;

SELECT d IN (materialize(x)), d NOT IN (materialize(x))
FROM values('d Decimal(9,2), x Float64', ('0.29', 0.29), ('0.57', 0.57))
ORDER BY d;
