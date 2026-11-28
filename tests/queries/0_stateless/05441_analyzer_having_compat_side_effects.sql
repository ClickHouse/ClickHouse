-- With `analyzer_compatibility_allow_non_aggregate_in_having`, a conjunct on a non-grouped column is moved to `WHERE`,
-- like in the legacy rewrite, even if a sibling conjunct has a function with observable side effects.

SET enable_analyzer = 1;

SELECT d, sum(x)
FROM values('k UInt64, d Date, x UInt64', (0, '2024-01-01', 1), (1, '2024-01-01', 10), (0, '2024-01-01', 2))
GROUP BY d
HAVING k = 0 AND sleepEachRow(0) = 0
SETTINGS analyzer_compatibility_allow_non_aggregate_in_having = 1, query_plan_enable_optimizations = 0;

-- The same with the function with observable side effects in a subquery.
SELECT d, sum(x)
FROM values('k UInt64, d Date, x UInt64', (0, '2024-01-01', 1), (1, '2024-01-01', 10), (0, '2024-01-01', 2))
GROUP BY d
HAVING k = 0 AND (SELECT sleepEachRow(0)) = 0
SETTINGS analyzer_compatibility_allow_non_aggregate_in_having = 1, query_plan_enable_optimizations = 0;
