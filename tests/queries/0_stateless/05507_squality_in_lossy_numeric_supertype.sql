SELECT
    x,
    toInt64(0) IN (toInt64(65), x),
    toInt64(0) NOT IN (toInt64(65), x)
FROM values('x Float64', (0.5), (1.5), (-0.5))
ORDER BY x
SETTINGS allow_lossy_numeric_supertype = 1;
