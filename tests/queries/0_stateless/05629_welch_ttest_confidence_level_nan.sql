-- welchTTest with a confidence level returns NaN, not an exception, when the t-statistic is NaN.
-- https://github.com/ClickHouse/ClickHouse/issues/130727

SELECT 'nan', studentTTest(0.95)(x, g), welchTTest(0.95)(x, g) FROM VALUES('x Float64, g UInt8', (1, 0), (nan, 0), (3, 0), (4, 1), (5, 1), (6, 1));
SELECT 'inf', studentTTest(0.95)(x, g), welchTTest(0.95)(x, g) FROM VALUES('x Float64, g UInt8', (1, 0), (inf, 0), (3, 0), (4, 1), (5, 1), (6, 1));
SELECT 'zeros', studentTTest(0.95)(x, g), welchTTest(0.95)(x, g) FROM VALUES('x Float64, g UInt8', (0, 0), (0, 0), (0, 0), (0, 1), (0, 1), (0, 1));
SELECT 'group by', g2, welchTTest(0.95)(x, g) FROM VALUES('x Float64, g UInt8, g2 UInt8', (1, 0, 0), (nan, 0, 0), (3, 0, 0), (4, 1, 0), (5, 1, 0), (6, 1, 0), (0, 0, 1), (0, 0, 1), (0, 1, 1), (0, 1, 1)) GROUP BY g2 ORDER BY g2;
SELECT 'finite', round(r.1, 6), round(r.2, 6), round(r.3, 6), round(r.4, 6) FROM (SELECT welchTTest(0.95)(x, g) AS r FROM VALUES('x Float64, g UInt8', (1, 0), (2, 0), (4, 0), (4, 1), (5, 1), (7, 1)));
