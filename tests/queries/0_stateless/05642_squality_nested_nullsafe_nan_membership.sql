SELECT x IN (materialize(x)), x NOT IN (materialize(x)), x IN (x), isNotDistinctFrom(x, x)
FROM (SELECT map('a', tuple(toFloat64('nan'), CAST(NULL AS Nullable(Int64)))) AS x)
SETTINGS transform_null_in = 1, allow_lossy_numeric_supertype = 0;

SELECT x IN (materialize(x)), x NOT IN (materialize(x)), x IN (x), isNotDistinctFrom(x, x)
FROM (SELECT map('a', tuple(toFloat64('nan'), CAST(NULL AS Nullable(Int64)))) AS x)
SETTINGS transform_null_in = 1, allow_lossy_numeric_supertype = 1;

SELECT x IN (materialize(x)), x NOT IN (materialize(x)), x IN (x)
FROM (SELECT map('a', [tuple(toFloat64('nan'), CAST(NULL AS Nullable(Int64)))]) AS x)
SETTINGS transform_null_in = 1, allow_lossy_numeric_supertype = 0;

SELECT count() FROM
(SELECT map('a', tuple(toFloat64('nan'), CAST(NULL AS Nullable(Int64)))) AS x)
WHERE x IN (materialize(x))
SETTINGS transform_null_in = 1, allow_lossy_numeric_supertype = 0;

SELECT x IN (materialize(x)), x NOT IN (materialize(x)), x IN (x)
FROM values('x Float64', ('nan'))
SETTINGS transform_null_in = 1, allow_lossy_numeric_supertype = 0;
