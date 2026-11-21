SET transform_null_in = 1;
SET allow_lossy_numeric_supertype = 0;

SELECT x IN (materialize(x)), x NOT IN (materialize(x)), isNotDistinctFrom(x, x),
       x IN (CAST('nan' AS Nullable(Float64)))
FROM values('x Nullable(Float64)', ('nan'));

SET allow_lossy_numeric_supertype = 1;

SELECT x IN (materialize(x)), x NOT IN (materialize(x)), isNotDistinctFrom(x, x),
       x IN (CAST('nan' AS Nullable(Float64)))
FROM values('x Nullable(Float64)', ('nan'));

SET allow_lossy_numeric_supertype = 0;

SELECT x IN (materialize(x)), x NOT IN (materialize(x)), isNotDistinctFrom(x, x)
FROM (SELECT tuple(CAST('nan' AS Nullable(Float64)), 1) AS x);

SELECT x IN (materialize(x)), x NOT IN (materialize(x)), isNotDistinctFrom(x, x)
FROM (SELECT CAST(tuple(CAST(NULL AS Nullable(Int32)), 1) AS Dynamic) AS x);

SELECT x IN (materialize(x)), x NOT IN (materialize(x)), isNotDistinctFrom(x, x)
FROM (SELECT CAST(tuple(CAST(NULL AS Nullable(Int32)), 1) AS Variant(Tuple(Nullable(Int32), UInt8), String)) AS x);

SELECT x IN (materialize(x)), x NOT IN (materialize(x)), isNotDistinctFrom(x, x)
FROM (SELECT map('a', CAST(tuple(CAST(NULL AS Nullable(Int32)), 1) AS Dynamic)) AS x);

SELECT d IN (materialize(i)), d NOT IN (materialize(i))
FROM values('d Nullable(Decimal(18,0)), i UInt64', (1, '18446744073709551615'));
