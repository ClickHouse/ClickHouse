-- A run without a memory limit records the sizes of its sets in the statistics of hash tables. The same query with
-- `max_bytes_before_external_group_by` must still spill within its memory limit instead of allocating these sets.

SET max_threads = 2, count_distinct_optimization = 0, optimize_uniq_exact_radix_partitioning = 1,
    collect_hash_table_stats_during_aggregation = 1, max_bytes_ratio_before_external_group_by = 0;

SELECT uniqExact(toUInt64(if(number < 20000000, number % 200000, number % 4000000))) FROM numbers_mt(60000000)
SETTINGS max_bytes_before_external_group_by = 0;

SELECT uniqExact(toUInt64(if(number < 20000000, number % 200000, number % 4000000))) FROM numbers_mt(60000000)
SETTINGS max_bytes_before_external_group_by = '8Mi', max_memory_usage = '64Mi';
