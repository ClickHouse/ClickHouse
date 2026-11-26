-- The ratio spill thresholds count the memory left under the query's `max_memory_usage`.

-- Needs more than its limit in memory, so it has to spill to finish.
SELECT count() FROM (SELECT number AS k, count() FROM numbers(5000000) GROUP BY k)
SETTINGS max_memory_usage = '100Mi', max_bytes_ratio_before_external_group_by = 0.3, max_bytes_before_external_group_by = 0,
    min_bytes_before_external_group_by = 0, collect_hash_table_stats_during_aggregation = 0, max_threads = 2, log_comment = '05843_group_by';

-- Fits in its limit, but its right side is above 0.1 of it.
SELECT count() FROM numbers(1000) AS l INNER JOIN numbers(2000000) AS r ON l.number = r.number
SETTINGS max_memory_usage = '400Mi', max_bytes_ratio_before_external_join = 0.1, max_bytes_before_external_join = 0,
    join_algorithm = 'hash', query_plan_join_swap_table = 0, max_threads = 2, log_comment = '05843_join';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['ExternalAggregationWritePart'] > 0, ProfileEvents['ExternalJoinWritePart'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05843_group_by', '05843_join')
ORDER BY log_comment;
