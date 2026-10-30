-- Tags: no-asan, no-tsan, no-msan
-- Tag no-asan, no-tsan, no-msan: the sanitizers replace `operator new`, so the allocations of the window through it are not tracked

-- `max_memory_usage` must stop a window function over the whole input near the limit. These block and batch sizes make the
-- allocations that are checked against the limit reach the memory tracker together with ones that are not.
SELECT any(isNull(zero)) OVER () FROM zeros(30000000) FORMAT Null
SETTINGS max_memory_usage = '100Mi', max_block_size = 65409, max_untracked_memory = '4Mi', memory_profiler_step = '4Mi',
    max_rows_to_read = 0, log_comment = '05453_window'; -- { serverError MEMORY_LIMIT_EXCEEDED }

SYSTEM FLUSH LOGS query_log;

SELECT parseReadableSize(extract(exception, 'would use ([0-9.]+ [KMGT]?i?B)')) < 150 * 1024 * 1024
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05453_window' AND type = 'ExceptionWhileProcessing' AND event_date >= yesterday();
