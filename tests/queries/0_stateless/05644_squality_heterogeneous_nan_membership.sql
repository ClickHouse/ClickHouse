SET transform_null_in = 1;
SET allow_lossy_numeric_supertype = 1;

SELECT x IN (x),
       x IN (x, toInt64(65)),
       x NOT IN (x, toInt64(65)),
       x IN (x, toFloat64(65))
FROM values('x Nullable(Float64)', ('nan'));

SELECT count() FROM values('x Nullable(Float64)', ('nan'))
WHERE x IN (x, toInt64(65));

SELECT count() FROM values('x Nullable(Float64)', ('nan'))
WHERE x NOT IN (x, toInt64(65));
