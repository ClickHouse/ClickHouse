-- Tags: no-fasttest
-- no-fasttest: compiled without datasketches

-- 2000 groups whose sketches grow to 64 KiB each (125 MiB) after the groups exist, so no new group or arena chunk checks the
-- limit in between: the sketches themselves must be refused.
SELECT number % 2000 AS k, uniqTheta(number) FROM numbers(1200000) GROUP BY k FORMAT Null
SETTINGS max_threads = 1, max_block_size = 1200000, max_bytes_before_external_group_by = 0,
    max_bytes_ratio_before_external_group_by = 0, max_memory_usage = '60M', log_comment = 'uniq_theta_memory_limit_insert'; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- The same for the sketches that merge states: every group merges a state of 600 values.
WITH (SELECT uniqThetaState(number) FROM numbers(600)) AS s
SELECT toUInt16(number) AS k, uniqThetaMerge(s) FROM numbers(1700) GROUP BY k FORMAT Null
SETTINGS max_threads = 1, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0,
    max_memory_usage = '60M', log_comment = 'uniq_theta_memory_limit_merge'; -- { serverError MEMORY_LIMIT_EXCEEDED }

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, memory_usage < 80000000
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday()
    AND type = 'ExceptionWhileProcessing' AND startsWith(log_comment, 'uniq_theta_memory_limit_')
ORDER BY log_comment;
