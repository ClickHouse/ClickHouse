SET max_threads = 1, max_block_size = 2, enable_parallel_replicas = 0;
SET max_rows_to_group_by = 1, group_by_overflow_mode = 'any';
SET group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 0;
SET max_bytes_ratio_before_external_group_by = 0, max_untracked_memory = 0;
SET enable_adaptive_aggregator = 0;

-- Every block after the second one brings keys that were never admitted. The
-- first block stays under the limit, the second crosses it.
SELECT if(number < 2, toUInt64(0), toUInt64(number - 1)) AS k, count(), sum(number)
FROM numbers(20)
GROUP BY k ORDER BY k
SETTINGS max_bytes_before_external_group_by = 0;

-- A spill before the limit is reached must not let the later keys in: the keys
-- of the flush are admitted ones, so the limit counts them too.
SELECT if(number < 2, toUInt64(0), toUInt64(number - 1)) AS k, count(), sum(number)
FROM numbers(20)
GROUP BY k ORDER BY k
SETTINGS max_bytes_before_external_group_by = 1,
         log_queries = 1, log_profile_events = 1,
         log_comment = '05408_external_aggregation_overflow_any_cap_survives_spill';

SYSTEM FLUSH LOGS query_log;
SELECT max(ProfileEvents['ExternalAggregationWritePart']) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '05408_external_aggregation_overflow_any_cap_survives_spill'
    AND type = 'QueryFinish'
    AND event_date >= yesterday();
