-- Frames outside the main binary are stored as 0 in `system.trace_log.trace`; `flameGraph` skips them
-- instead of ending the trace, which lost every frame after the first one (`start_thread` comes first).
SET allow_introspection_functions = 1;

SELECT arrayJoin(flameGraph(trace))
FROM (SELECT [0, 2, 0, 3]::Array(UInt64) AS trace UNION ALL SELECT [2, 4]::Array(UInt64))
ORDER BY 1;
