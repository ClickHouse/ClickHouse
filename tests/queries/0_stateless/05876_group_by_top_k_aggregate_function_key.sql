-- `GROUP BY ... LIMIT` ranks the keys to skip rows; aggregate state keys all rank equal, so no row is skipped.
SET max_threads = 1, optimize_trivial_group_by_limit_query = 0, enable_group_by_top_k_optimization = 1, enable_group_by_top_k_dynamic_filtering = 0;

SELECT count() FROM (SELECT arrayReduce('sumState', [number]) AS s FROM numbers(2)) GROUP BY s LIMIT 1;

-- Every group has 5 rows.
SELECT count() FROM (SELECT number % 3 AS k, arrayReduce('sumState', [number % 2]) AS s FROM numbers(30)) GROUP BY s, k LIMIT 1;
