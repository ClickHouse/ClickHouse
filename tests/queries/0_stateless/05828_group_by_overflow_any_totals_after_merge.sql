-- `totals_mode = 'before_having'` counts the groups that do not pass `max_rows_to_group_by` in the totals, also those that the merge
-- of the per-thread tables does not add. Every block holds one key, so each thread has keys of its own.
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 1000,
    max_rows_to_group_by = 100, group_by_overflow_mode = 'any', totals_mode = 'before_having',
    group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0,
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT intDiv(number, 1000) AS k, count() FROM numbers_mt(2000000) GROUP BY k WITH TOTALS HAVING count() = 0;
SELECT intDiv(number, 1000) AS k, sum(number), uniqExact(number) FROM numbers_mt(2000000) GROUP BY k WITH TOTALS HAVING count() = 0;
SELECT toString(intDiv(number, 1000)) AS k, count() FROM numbers_mt(2000000) GROUP BY k WITH TOTALS HAVING count() = 0;
SELECT toNullable(intDiv(number, 1000)) AS k, count() FROM numbers_mt(2000000) GROUP BY k WITH TOTALS HAVING count() = 0;
SELECT toLowCardinality(toString(intDiv(number, 1000))) AS k, count() FROM numbers_mt(2000000) GROUP BY k WITH TOTALS HAVING count() = 0;
SELECT toUInt8(intDiv(number, 1000) % 256) AS k, count() FROM numbers_mt(2000000) GROUP BY k WITH TOTALS HAVING count() = 0;
