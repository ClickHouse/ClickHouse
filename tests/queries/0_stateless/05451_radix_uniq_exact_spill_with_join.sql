-- The hash join holds about 19 MiB while `uniqExact` with radix partitioning aggregates its output. The keys are written to
-- disk once the memory of the whole query is over `max_bytes_before_external_group_by`, as in the ordinary aggregation,
-- not only the memory of `uniqExact`, so the query stays within its limit.

-- `count_distinct_optimization` would rewrite `uniqExact` to a `GROUP BY`. The join must build its table from the small side.
SELECT uniqExact(a.number)
FROM numbers_mt(4000000) AS a
INNER JOIN (SELECT number AS k FROM numbers(400000)) AS b ON a.number % 400000 = b.k
SETTINGS max_threads = 2, max_block_size = 4096, max_threads_min_free_memory_per_thread = 0, join_algorithm = 'hash',
    query_plan_join_swap_table = false,
    count_distinct_optimization = 0, collect_hash_table_stats_during_aggregation = 0, optimize_uniq_exact_radix_partitioning = 1,
    max_untracked_memory = 0, max_bytes_ratio_before_external_group_by = 0, max_bytes_before_external_group_by = '24Mi',
    max_memory_usage = '40Mi';
