-- A LIMIT BY query that runs out of memory while it collects output rows must fail, not crash the server.
-- The keys (UInt8, UInt64) use the keys128 hash method, and `a` alternates, so every row is a run of its own
-- and the output slices grow row by row. The limits below are crossed by that growth.

SELECT number, number % 2 AS a, intDiv(number, 1000000000) AS b FROM numbers(4194304) LIMIT 1000000000 BY a, b
SETTINGS max_threads = 1, max_block_size = 4194304, max_memory_usage = '256Mi', log_comment = '05876_limit_by_oom',
    max_untracked_memory = 1048576, memory_profiler_step = 1048576, memory_tracker_fault_probability = 0,
    memory_overcommit_ratio_denominator = 0, memory_usage_overcommit_max_wait_microseconds = 0, calculate_text_stack_trace = 1
FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

SELECT number, number % 2 AS a, intDiv(number, 1000000000) AS b FROM numbers(4194304) LIMIT 1000000000 BY a, b
SETTINGS max_threads = 1, max_block_size = 4194304, max_memory_usage = '272Mi', log_comment = '05876_limit_by_oom',
    max_untracked_memory = 1048576, memory_profiler_step = 1048576, memory_tracker_fault_probability = 0,
    memory_overcommit_ratio_denominator = 0, memory_usage_overcommit_max_wait_microseconds = 0, calculate_text_stack_trace = 1
FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

SELECT number, number % 2 AS a, intDiv(number, 1000000000) AS b FROM numbers(4194304) LIMIT 1000000000 BY a, b
SETTINGS max_threads = 1, max_block_size = 4194304, max_memory_usage = '288Mi', log_comment = '05876_limit_by_oom',
    max_untracked_memory = 1048576, memory_profiler_step = 1048576, memory_tracker_fault_probability = 0,
    memory_overcommit_ratio_denominator = 0, memory_usage_overcommit_max_wait_microseconds = 0, calculate_text_stack_trace = 1
FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

SYSTEM FLUSH LOGS query_log;

SELECT countIf(position(stack_trace, 'LimitByTransform::appendOutputSliceWithGrowth') > 0) >= 1
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
    AND log_comment = '05876_limit_by_oom' AND type = 'ExceptionWhileProcessing';
