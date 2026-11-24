SET max_threads = 1, max_block_size = 2, enable_parallel_replicas = 0;
SET max_rows_to_group_by = 1, group_by_overflow_mode = 'any';
SET group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 0;
SET max_bytes_ratio_before_external_group_by = 0, max_untracked_memory = 0;
SET enable_adaptive_aggregator = 0;

-- The first block admits both keys before `any` stops admitting new ones.
-- `UInt64` selects a hash table that supports external aggregation.
SELECT toUInt64(number % 2) AS k, count()
FROM numbers(4)
GROUP BY k ORDER BY k
SETTINGS max_bytes_before_external_group_by = 0;

-- A spill between the two blocks must preserve the admitted keys, so the second
-- occurrence of each key still contributes to its count.
SELECT toUInt64(number % 2) AS k, count()
FROM numbers(4)
GROUP BY k ORDER BY k
SETTINGS max_bytes_before_external_group_by = 1,
         log_queries = 1, log_profile_events = 1,
         log_comment = '05401_external_aggregation_overflow_any_keeps_kept_keys';

SYSTEM FLUSH LOGS query_log;
SELECT max(ProfileEvents['ExternalAggregationWritePart']) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '05401_external_aggregation_overflow_any_keeps_kept_keys'
    AND type = 'QueryFinish'
    AND event_date >= yesterday();
