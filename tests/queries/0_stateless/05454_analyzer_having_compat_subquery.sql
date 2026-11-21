-- With `analyzer_compatibility_allow_non_aggregate_in_having`, a conjunct with a column that is not grouped is moved
-- to `WHERE` even if it has a subquery with a non-deterministic function, like with the old analyzer.

SET enable_analyzer = 1;

SELECT d, sum(x)
FROM values('k UInt64, d Date, x UInt64', (0, '2024-01-01', 1), (1, '2024-01-01', 10), (0, '2024-01-01', 2))
GROUP BY d
HAVING k IN (SELECT rand() % 1)
SETTINGS analyzer_compatibility_allow_non_aggregate_in_having = 1, query_plan_enable_optimizations = 0;

SELECT d, sum(x)
FROM values('k UInt64, d Date, x UInt64', (0, '2024-01-01', 1), (1, '2024-01-01', 10), (0, '2024-01-01', 2))
GROUP BY d
HAVING k IN (SELECT rowNumberInAllBlocks())
SETTINGS analyzer_compatibility_allow_non_aggregate_in_having = 1;

SELECT d, sum(x)
FROM values('k UInt64, d Date, x UInt64', (0, '2024-01-01', 1), (1, '2024-01-01', 10), (0, '2024-01-01', 2))
GROUP BY d
HAVING k IN (SELECT number % 1 FROM generateRandom('number UInt64') LIMIT 1)
SETTINGS analyzer_compatibility_allow_non_aggregate_in_having = 1;
