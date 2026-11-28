-- TOTALS under `group_by_overflow_mode = 'any'` count every input row when the per-thread single-level tables
-- are merged: the keys refused by `max_rows_to_group_by` during the merge go into the overflow row.

SET max_block_size = 1000, max_rows_to_group_by = 100, group_by_overflow_mode = 'any', totals_mode = 'before_having',
    group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0,
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0,
    max_threads_min_free_memory_per_thread = 0;

SELECT intDiv(number, 1000) AS k, count() FROM numbers_mt(2000000) GROUP BY k WITH TOTALS HAVING 0 ORDER BY k SETTINGS max_threads = 1;
SELECT intDiv(number, 1000) AS k, count() FROM numbers_mt(2000000) GROUP BY k WITH TOTALS HAVING 0 ORDER BY k SETTINGS max_threads = 4;
SELECT toString(intDiv(number, 1000)) AS k, sum(number) FROM numbers_mt(2000000) GROUP BY k WITH TOTALS HAVING 0 ORDER BY k SETTINGS max_threads = 4;
SELECT intDiv(number, 1000) AS k, uniqExact(number) FROM numbers_mt(2000000) GROUP BY k WITH TOTALS HAVING 0 ORDER BY k SETTINGS max_threads = 4;
