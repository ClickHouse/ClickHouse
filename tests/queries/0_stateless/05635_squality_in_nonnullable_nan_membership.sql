SET transform_null_in = 1;
SET allow_lossy_numeric_supertype = 1;

SELECT x IN (materialize(x)), x NOT IN (materialize(x)),
       x IN (x), x NOT IN (x),
       x IN (CAST('nan' AS Float64)), x NOT IN (CAST('nan' AS Float64))
FROM values('x Float64', ('nan'));

SET allow_lossy_numeric_supertype = 0;

SELECT x IN (materialize(x)), x NOT IN (materialize(x)),
       x IN (x), x NOT IN (x)
FROM values('x Float64', ('nan'));
