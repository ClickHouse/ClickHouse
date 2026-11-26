-- studentTTest with a confidence level uses the pooled standard error for the interval, like scipy.stats.ttest_ind(equal_var=True).
-- https://github.com/ClickHouse/ClickHouse/issues/133315

SELECT 'unequal', arrayMap(v -> round(v, 6), [r.1, r.2, r.3, r.4]) FROM (SELECT studentTTest(0.95)(x, g) AS r FROM VALUES('x Float64, g UInt8', (1, 0), (2, 0), (3, 0), (0, 1), (10, 1)));
SELECT 'single row', arrayMap(v -> round(v, 6), [r.1, r.2, r.3, r.4]) FROM (SELECT studentTTest(0.95)(x, g) AS r FROM VALUES('x Float64, g UInt8', (5, 0), (1, 1), (2, 1), (3, 1)));
SELECT 'equal', arrayMap(v -> round(v, 6), [r.1, r.2, r.3, r.4]) FROM (SELECT studentTTest(0.95)(x, g) AS r FROM VALUES('x Float64, g UInt8', (1, 0), (2, 0), (4, 0), (4, 1), (5, 1), (9, 1)));
SELECT 'welch', arrayMap(v -> round(v, 6), [r.1, r.2, r.3, r.4]) FROM (SELECT welchTTest(0.95)(x, g) AS r FROM VALUES('x Float64, g UInt8', (1, 0), (2, 0), (3, 0), (0, 1), (10, 1)));
