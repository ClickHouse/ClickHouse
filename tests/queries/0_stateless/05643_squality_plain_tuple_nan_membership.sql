SELECT x IN (s), x NOT IN (s),
       isNotDistinctFrom(x, CAST(s AS Tuple(Float64, Nullable(Int64))))
FROM values('x Tuple(Float64, Nullable(Int64)), s String', (('nan', 1), '(nan,1)'))
SETTINGS transform_null_in = 1, allow_lossy_numeric_supertype = 0;

SELECT x IN (s), x NOT IN (s)
FROM values('x Tuple(Float64, Nullable(Int64)), s String', (('nan', 1), '(nan,1)'))
SETTINGS transform_null_in = 1, allow_lossy_numeric_supertype = 1;

SELECT count() FROM values('x Tuple(Float64, Nullable(Int64)), s String', (('nan', 1), '(nan,1)'))
WHERE x IN (s)
SETTINGS transform_null_in = 1, allow_lossy_numeric_supertype = 0;
