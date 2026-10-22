-- A run without a memory limit records 200000 distinct keys per stream in the statistics of hash tables. Their sets take
-- 32 MiB, over `max_bytes_before_external_group_by`, so the same query with it must not allocate them from the start.

SET max_threads = 8, max_threads_min_free_memory_per_thread = 0, count_distinct_optimization = 0,
    optimize_uniq_exact_radix_partitioning = 1, collect_hash_table_stats_during_aggregation = 1,
    max_bytes_ratio_before_external_group_by = 0;

SELECT uniqExact(toUInt64(number % 200000)) FROM numbers_mt(20000000) SETTINGS max_bytes_before_external_group_by = 0;

SELECT uniqExact(toUInt64(number % 200000)) FROM numbers_mt(20000000)
SETTINGS max_bytes_before_external_group_by = '25Mi', max_memory_usage = '44Mi', max_untracked_memory = 0;
