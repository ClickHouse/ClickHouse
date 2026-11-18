-- Tags: no-parallel, no-fasttest
-- Tag no-parallel: the failpoint is global and would fire in other queries.
-- Tag no-fasttest: failpoints are not available in the fast test build.

-- `ColumnAggregateFunction::expand` (used by the short-circuit evaluation of `if`) must stay valid when
-- creating an aggregate function state throws, so that the column can be destroyed.

SYSTEM ENABLE FAILPOINT column_aggregate_function_expand_exception;

SELECT finalizeAggregation(if(number % 3 = 0,
    initializeAggregation('groupArrayState', number),
    initializeAggregation('groupArrayState', number + 1)))
FROM numbers(100)
FORMAT Null
SETTINGS short_circuit_function_evaluation = 'force_enable'; -- { serverError MEMORY_LIMIT_EXCEEDED }

SYSTEM DISABLE FAILPOINT column_aggregate_function_expand_exception;

SELECT 'ok';
