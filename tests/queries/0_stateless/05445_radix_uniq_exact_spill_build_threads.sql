-- With `max_bytes_before_external_group_by`, `uniqExact` with radix partitioning builds its buckets, which read the
-- spilled keys back, in at most `aggregation_memory_efficient_merge_threads` threads, as the ordinary aggregation merges
-- its temporary data.

SET max_threads = 4, count_distinct_optimization = 0, optimize_uniq_exact_radix_partitioning = 1,
    max_bytes_before_external_group_by = '8Mi';

SELECT trimBoth(explain) FROM (EXPLAIN PIPELINE SELECT uniqExact(number) FROM numbers_mt(1000))
WHERE explain LIKE '%RadixUniqExactBuildTransform%'
SETTINGS aggregation_memory_efficient_merge_threads = 1;

SELECT trimBoth(explain) FROM (EXPLAIN PIPELINE SELECT uniqExact(number) FROM numbers_mt(1000))
WHERE explain LIKE '%RadixUniqExactBuildTransform%'
SETTINGS aggregation_memory_efficient_merge_threads = 0;
