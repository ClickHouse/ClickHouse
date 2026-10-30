-- `max_memory_usage` must stop a query near the limit even when it grows by many small allocations, here the keys
-- buffered by `uniqExact` with radix partitioning. 20 million distinct keys cannot fit into 32 MiB.

-- `count_distinct_optimization` would rewrite `uniqExact` to a `GROUP BY`.
SET max_threads = 2, collect_hash_table_stats_during_aggregation = 0, max_bytes_before_external_group_by = 0, count_distinct_optimization = 0;

SELECT uniqExact(number) FROM numbers_mt(20000000)
SETTINGS optimize_uniq_exact_radix_partitioning = 1, max_memory_usage = '32Mi', log_comment = '05441_radix'; -- { serverError MEMORY_LIMIT_EXCEEDED }

SYSTEM FLUSH LOGS query_log;

-- The query stops soon after it reaches the limit.
SELECT parseReadableSize(extract(exception, 'would use ([0-9.]+ [KMGT]?i?B)')) < 100 * 1024 * 1024
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05441_radix' AND type = 'ExceptionWhileProcessing' AND event_date >= yesterday();
