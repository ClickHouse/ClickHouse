-- Tags: no-old-analyzer

-- With `join_algorithm = 'auto'`, a hash join that outgrows its limits switches to `MergeJoin`.
-- Here creating the `MergeJoin` throws. The plan of the failed query is logged with its statistics,
-- which read the join's report, so the join left installed must still be a usable one.

SELECT count()
FROM numbers(10) AS l
ALL INNER JOIN numbers(10) AS r ON l.number = r.number
SETTINGS log_query_plans = 1, join_algorithm = 'auto',
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    max_bytes_in_join = 1, partial_merge_join_rows_in_right_blocks = 0,
    query_plan_join_swap_table = 'false', max_threads = 1,
    log_comment = '05877_join_switch'
FORMAT Null; -- { serverError PARAMETER_OUT_OF_BOUND }

SYSTEM FLUSH LOGS query_log;

SELECT
    type,
    isValidJSON(toJSONString(query_plan)),
    JSONExtractUInt(toJSONString(query_plan), 'ExecutionTimeNs') > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '05877_join_switch'
  AND type = 'ExceptionWhileProcessing';
