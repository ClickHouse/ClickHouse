-- `uniqExact` with radix partitioning writes its keys to disk with `max_bytes_before_external_group_by`, and computes
-- the same result within a memory limit its keys alone exceed.

-- `count_distinct_optimization` would rewrite `uniqExact` to a `GROUP BY`.
SET max_threads = 2, collect_hash_table_stats_during_aggregation = 0, optimize_uniq_exact_radix_partitioning = 1, count_distinct_optimization = 0;

-- 20 million distinct keys take 160 MB.
SELECT uniqExact(number) FROM numbers_mt(20000000)
SETTINGS max_bytes_before_external_group_by = '16Mi', max_memory_usage = '100Mi', log_comment = '05443_numbers';

-- Frequent values mixed with distinct ones.
SELECT uniqExact(if(number % 10 = 0, number, number % 3)) FROM numbers_mt(200000000)
SETTINGS max_bytes_before_external_group_by = '16Mi', max_memory_usage = '100Mi', log_comment = '05443_frequent';

-- Strings, whose keys are their 128-bit hashes.
SELECT uniqExact(toString(number)) FROM numbers_mt(5000000)
SETTINGS max_bytes_before_external_group_by = '16Mi', max_memory_usage = '100Mi', log_comment = '05443_strings';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['ExternalAggregationWritePart'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('05443_numbers', '05443_frequent', '05443_strings') AND type = 'QueryFinish'
    AND event_date >= yesterday()
ORDER BY log_comment;
