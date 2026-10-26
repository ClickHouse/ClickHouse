SET max_threads = 1, max_block_size = 2, enable_parallel_replicas = 0;
SET max_rows_to_group_by = 1, group_by_overflow_mode = 'any';
SET group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 0;
SET max_bytes_ratio_before_external_group_by = 0, max_untracked_memory = 0;
SET enable_adaptive_aggregator = 0;

-- Key layout per block: [0, 0], [1, 2], [0, 0]. The first block stays under the
-- limit, the second crosses it, and the third brings back the key of the first.
SELECT multiIf(number < 2, toUInt64(0),
               number = 2, toUInt64(1),
               number = 3, toUInt64(2), toUInt64(0)) AS k,
       count(), sum(number)
FROM numbers(6)
GROUP BY k ORDER BY k
SETTINGS max_bytes_before_external_group_by = 0;

-- The first block is spilled before the limit is reached, so its key is no longer
-- in the table: dropping the rows of the third block would undercount a key that
-- the temporary file still returns.
SELECT multiIf(number < 2, toUInt64(0),
               number = 2, toUInt64(1),
               number = 3, toUInt64(2), toUInt64(0)) AS k,
       count(), sum(number)
FROM numbers(6)
GROUP BY k ORDER BY k
SETTINGS max_bytes_before_external_group_by = 1,
         log_comment = '05407_external_aggregation_overflow_any_spill_before_cap';

SYSTEM FLUSH LOGS query_log;
SELECT max(ProfileEvents['ExternalAggregationWritePart']) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '05407_external_aggregation_overflow_any_spill_before_cap'
    AND type = 'QueryFinish'
    AND event_date >= yesterday();
