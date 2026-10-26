-- With many streams, each one keeps a small set of distinct keys before it starts writing them to disk. Together these sets
-- exceed the memory limit, so they must spill with `max_bytes_before_external_group_by` too.

-- `count_distinct_optimization` would rewrite `uniqExact` to a `GROUP BY`.
SELECT uniqExact(number) FROM numbers_mt(2000000)
SETTINGS max_threads = 32, max_threads_min_free_memory_per_thread = 0, max_block_size = 4096, max_untracked_memory = 0,
    count_distinct_optimization = 0, collect_hash_table_stats_during_aggregation = 0, optimize_uniq_exact_radix_partitioning = 1,
    max_bytes_ratio_before_external_group_by = 0, max_bytes_before_external_group_by = '4Mi', max_memory_usage = '32Mi';
