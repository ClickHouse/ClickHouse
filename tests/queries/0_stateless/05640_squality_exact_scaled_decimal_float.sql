SET allow_lossy_numeric_supertype = 1;

SELECT d, x IN (d, y), x NOT IN (d, y),
       x IN (materialize(d)), x NOT IN (materialize(d))
FROM values('d Decimal(18,17), x Float64, y Int64',
    ('1.00000762939453125', 1.00000762939453125, 65),
    ('1.00000762939453120', 1.00000762939453125, 65))
ORDER BY d DESC
SETTINGS transform_null_in = 0;

SELECT d, x IN (d, y), x NOT IN (d, y),
       x IN (materialize(d)), x NOT IN (materialize(d))
FROM values('d Decimal(18,17), x Float64, y Int64',
    ('1.00000762939453125', 1.00000762939453125, 65),
    ('1.00000762939453120', 1.00000762939453125, 65))
ORDER BY d DESC
SETTINGS transform_null_in = 1;

SELECT count() FROM values('d Decimal(18,17), x Float64',
    ('1.00000762939453125', 1.00000762939453125),
    ('1.00000762939453120', 1.00000762939453125))
WHERE x IN (materialize(d));
