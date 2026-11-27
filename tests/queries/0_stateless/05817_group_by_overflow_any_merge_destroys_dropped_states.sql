-- Under group_by_overflow_mode = 'any' the merge of the per-thread tables drops the keys the first table does not have.
-- Their uniqExact states own heap memory, so a state that is not destroyed is reported as a leak by sanitizer builds.

SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 1000,
    max_rows_to_group_by = 100, group_by_overflow_mode = 'any',
    group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0,
    collect_hash_table_stats_during_aggregation = 0,
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT intDiv(number, 1000) AS k, uniqExact(number) FROM numbers_mt(2000000) GROUP BY k FORMAT Null;
SELECT toString(intDiv(number, 1000)) AS k, uniqExact(number) FROM numbers_mt(2000000) GROUP BY k FORMAT Null;
-- The overflow row is emitted before the tables are merged, so the merge drops the states here too.
SELECT intDiv(number, 1000) AS k, uniqExact(number) FROM numbers_mt(2000000) GROUP BY k WITH TOTALS FORMAT Null SETTINGS totals_mode = 'before_having';

SYSTEM FLUSH LOGS query_log;
SELECT count() FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND has(used_aggregate_functions, 'uniqExact') AND ProfileEvents['OverflowAny'] > 0;
