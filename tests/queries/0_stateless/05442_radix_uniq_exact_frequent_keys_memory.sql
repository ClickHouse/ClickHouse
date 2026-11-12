-- A few frequent values that are not consecutive, among many distinct ones, must not make `uniqExact` with radix
-- partitioning keep a copy of every row: 200002 distinct values fit into a few MiB.

SET max_threads = 2, collect_hash_table_stats_during_aggregation = 0;

SELECT uniqExact(if(number % 100 = 0, number, number % 3)) FROM numbers_mt(20000000)
SETTINGS optimize_uniq_exact_radix_partitioning = 1, max_memory_usage = '100Mi';

SELECT uniqExact(if(number % 100 = 0, toString(number), toString(number % 3))) FROM numbers_mt(10000000)
SETTINGS optimize_uniq_exact_radix_partitioning = 1, max_memory_usage = '100Mi';
