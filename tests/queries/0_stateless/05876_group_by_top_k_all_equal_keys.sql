-- `GROUP BY ... LIMIT` ranks the keys to skip rows. Aggregate states and `Nothing` values all rank equal, so no row is skipped.
-- One row per block, so that the blocks after the first compare their rows with the boundary all at once.
SET max_threads = 1, max_block_size = 1, max_rows_to_group_by = 0, optimize_trivial_group_by_limit_query = 0, enable_group_by_top_k_optimization = 1, enable_group_by_top_k_dynamic_filtering = 0;

SELECT count() FROM (SELECT arrayReduce('sumState', [number]) AS s FROM numbers(2)) GROUP BY s LIMIT 1;

-- Every group has 5 rows.
SELECT count() FROM (SELECT number % 3 AS k, arrayReduce('sumState', [number % 2]) AS s FROM numbers(30)) GROUP BY s, k LIMIT 1;

-- All rows are in the one `Nothing` group. `format` returns them in one block, so they are compared one by one.
SELECT count() FROM format(Values, 'x Nothing', concat(repeat('(DEFAULT), ', 19999), '(DEFAULT)')) GROUP BY x LIMIT 1;
