-- Single-value states over FixedString allocate their values through the memory limit.
SELECT sum(c) FROM (SELECT number AS k, countArgMax(toFixedString(toString(number % 2), 65535)) AS c FROM numbers(1024) GROUP BY k)
SETTINGS max_threads = 1, max_block_size = 65536, max_memory_usage = '100M', max_untracked_memory = '1Mi',
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0; -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT sum(c) FROM (SELECT number AS k, argMax(number, toFixedString(toString(number % 2), 65535)) AS c FROM numbers(1024) GROUP BY k)
SETTINGS max_threads = 1, max_block_size = 65536, max_memory_usage = '100M', max_untracked_memory = '1Mi',
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0; -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT sum(length(m)) FROM (SELECT number AS k, max(f) AS m FROM (SELECT number, toFixedString(toString(number % 2), 65535) AS f FROM numbers(1024)) GROUP BY k)
SETTINGS max_threads = 1, max_block_size = 65536, max_memory_usage = '100M', max_untracked_memory = '1Mi',
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0; -- { serverError MEMORY_LIMIT_EXCEEDED }

SYSTEM FLUSH LOGS query_log;
SELECT count(), countIf(memory_usage <= 100000000) FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'ExceptionWhileProcessing' AND exception_code = 241;

SELECT hex(maxState(toFixedString('ab', 4))), hex(argMaxState(toFixedString('ab', 4), 1)), hex(argMaxState(1, toFixedString('ab', 4))), hex(countArgMaxState(toFixedString('ab', 4)));
SELECT finalizeAggregation(CAST(unhex('0161620000') AS AggregateFunction(max, FixedString(4))));
SELECT max(f), min(f), argMax(f, number), argMax(number, f), countArgMax(f), anyHeavy(toFixedString('x', 3)), singleValueOrNull(toFixedString('x', 3))
FROM (SELECT toFixedString(toString(number), 8) AS f, number FROM numbers(100000)) SETTINGS max_block_size = 1000, max_threads = 4;
SELECT maxMerge(s), minMerge(t) FROM (SELECT maxState(toFixedString(toString(number), 3)) AS s, minState(toFixedString(toString(number), 3)) AS t FROM numbers(1000) GROUP BY number % 7);
