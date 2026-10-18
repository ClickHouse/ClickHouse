-- With `analyzer_compatibility_allow_non_aggregate_in_having`, a conjunct on a non-grouped column is moved to `WHERE`,
-- like in the legacy rewrite, even if the projection has a stateful function computed before aggregation.

SET enable_analyzer = 1;

SELECT d, sum(rowNumberInAllBlocks())
FROM values('k UInt64, d Date', (0, '2024-01-01'), (1, '2024-01-01'), (0, '2024-01-01'))
GROUP BY d
HAVING k = 0
SETTINGS analyzer_compatibility_allow_non_aggregate_in_having = 1, query_plan_enable_optimizations = 0;

SELECT d, sum(rowNumberInAllBlocks())
FROM values('k UInt64, d Date', (0, '2024-01-01'), (1, '2024-01-01'), (0, '2024-01-01'))
GROUP BY d
HAVING k = 0
SETTINGS analyzer_compatibility_allow_non_aggregate_in_having = 0; -- { serverError NOT_AN_AGGREGATE }
