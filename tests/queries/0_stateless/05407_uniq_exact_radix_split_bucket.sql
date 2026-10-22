-- A bucket whose set would be larger than 2 MiB is partitioned again when it is built.
-- Without statistics and with up to 16 threads there are 256 buckets, and the set of 16-byte keys has 2 cells of 16 bytes
-- per key, so a bucket goes over 2 MiB above 2 MiB / 32 B = 65536 keys, that is above 256 * 65536 = 16.8M distinct keys.
SELECT uniqExact(toUInt128(number)) FROM numbers_mt(20000000)
SETTINGS max_threads = 4, optimize_uniq_exact_radix_partitioning = 1, collect_hash_table_stats_during_aggregation = 0,
    optimize_injective_functions_inside_uniq = 0;
