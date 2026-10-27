-- A run without a memory limit records 131073 distinct keys per stream in the statistics of hash tables. Their sets are
-- rounded up to 4 MiB each, 32 MiB for 8 streams, twice the 16 MiB that the keys alone take, and over
-- `max_bytes_before_external_group_by`, so the same query with it must not allocate them from the start.
-- Small blocks keep the memory of the rest of the query small.

SET max_threads = 8, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192, count_distinct_optimization = 0,
    optimize_uniq_exact_radix_partitioning = 1, collect_hash_table_stats_during_aggregation = 1,
    max_bytes_ratio_before_external_group_by = 0;

SELECT uniqExact(toUInt64(number % 131073)) FROM numbers_mt(20000000) SETTINGS max_bytes_before_external_group_by = 0;

SELECT uniqExact(toUInt64(number % 131073)) FROM numbers_mt(20000000)
SETTINGS max_bytes_before_external_group_by = '17Mi', max_memory_usage = '31Mi', max_untracked_memory = 0;
