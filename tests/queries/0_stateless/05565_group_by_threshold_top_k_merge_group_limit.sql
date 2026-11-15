-- The top-K threshold merge checks a throw-mode `max_rows_to_group_by` against the groups it merges. Each
-- `UNION ALL` lane is a single block of 2000 keys, which goes to a producer of its own, so no producer
-- exceeds the limit, while the merge builds the 10 best groups of each of the 256 buckets: 2560 groups. The
-- ordinary merge would throw too, so the query log shows that the threshold merge ran.

SET enable_adaptive_aggregator = 0, serialize_query_plan = 0, max_threads = 4, max_block_size = 8192;
SET group_by_two_level_threshold = 100, group_by_two_level_threshold_bytes = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT intDiv(number, 4) AS k, max(number) AS m
FROM (SELECT number FROM numbers(0, 8000) UNION ALL SELECT number FROM numbers(8000, 8000)
    UNION ALL SELECT number FROM numbers(16000, 8000) UNION ALL SELECT number FROM numbers(24000, 8000))
GROUP BY k ORDER BY m DESC LIMIT 10
SETTINGS max_rows_to_group_by = 2300, group_by_overflow_mode = 'throw'
FORMAT Null; -- { serverError TOO_MANY_ROWS }

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['AggregationThresholdTopKMerges'] > 0 FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase()
    AND type = 'ExceptionWhileProcessing';
