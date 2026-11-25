-- Tags: no-old-analyzer

-- A query that threw while running logs a plan whose `Failure` object names the processor that
-- threw and, through `NodeId` and `GroupId`, the step of the plan and the stage it belongs to. The
-- plan also carries the statistics measured until the query failed. The node is found by its id
-- and its type is checked, so nothing depends on the numbering of the steps.

SET log_query_plans = 1;

-- In the filter. An `ISimpleTransform` passes its exception on through its output port, and a
-- processor further down rethrows it.
SELECT number FROM numbers(10) WHERE throwIf(number = 5, '05876_where') = 0 SETTINGS log_comment = '05876_where' FORMAT Null; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- In the projection.
SELECT throwIf(number = 5, '05876_select') FROM numbers(10) SETTINGS log_comment = '05876_select' FORMAT Null; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- In the filter, with several streams running at once.
SELECT number FROM numbers_mt(1000000) WHERE throwIf(number = 500000, '05876_parallel') = 0 SETTINGS max_threads = 4, max_block_size = 1000, log_comment = '05876_parallel' FORMAT Null; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- Thrown directly by the work of a processor that is not an `ISimpleTransform`.
SELECT number % 1000 AS k, count() FROM numbers(100000) GROUP BY k SETTINGS max_rows_to_group_by = 10, group_by_overflow_mode = 'throw', log_comment = '05876_group_by' FORMAT Null; -- { serverError TOO_MANY_ROWS }

-- A query that succeeds has no `Failure`.
SELECT number FROM numbers(10) WHERE number > 5 SETTINGS log_comment = '05876_ok' FORMAT Null;

SET log_query_plans = 0;
SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    type,
    has_failure,
    processor,
    JSONExtractString(failed_node, 'NodeType') AS node_type,
    -- The stage is one of those the failed node reports statistics for.
    has(arrayMap(stage -> JSONExtractUInt(stage, 'GroupId'), JSONExtractArrayRaw(failed_node, 'Statistics', 'Stages')), group_id) AS group_in_stages,
    JSONExtractUInt(plan, 'ExecutionTimeNs') > 0 AS has_execution_time
FROM
(
    SELECT
        log_comment,
        type,
        plan,
        JSONHas(plan, 'Failure') AS has_failure,
        JSONExtractString(plan, 'Failure', 'Processor') AS processor,
        JSONExtractUInt(plan, 'Failure', 'GroupId') AS group_id,
        arrayFirst(
            node -> JSONExtractString(node, 'NodeId') = JSONExtractString(plan, 'Failure', 'NodeId'),
            JSONExtractArrayRaw(plan, 'Nodes')) AS failed_node
    FROM
    (
        SELECT log_comment, type, toJSONString(query_plan) AS plan
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND log_comment IN ('05876_where', '05876_select', '05876_parallel', '05876_group_by', '05876_ok')
          AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
    )
)
ORDER BY log_comment
FORMAT TSV;
