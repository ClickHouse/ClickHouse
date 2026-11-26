-- The adaptive aggregator splits large blocks into consecutive row ranges; `GROUP BY ... ORDER BY ... LIMIT` over them must finish.
SELECT toUInt64(number % 2) AS k, count()
FROM numbers_mt(2000000)
GROUP BY k
ORDER BY k
LIMIT 1
SETTINGS max_threads = 2, max_block_size = 1000000, enable_adaptive_aggregator = 1, enable_group_by_top_k_optimization = 1,
    max_rows_to_group_by = 0, collect_hash_table_stats_during_aggregation = 0;
