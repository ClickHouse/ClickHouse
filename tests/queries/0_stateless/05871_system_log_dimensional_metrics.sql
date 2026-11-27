-- Per-log dimensional metrics of the system log writer.

SELECT 1 FORMAT Null;
SYSTEM FLUSH LOGS query_log;

-- The flush above wrote at least the entries of the queries of this test.
SELECT metric, value > 0
FROM system.dimensional_metrics
WHERE metric IN ('system_log_flushes_total', 'system_log_flushed_rows_total')
    AND labels['database'] = 'system' AND labels['table'] = 'query_log'
    AND (metric != 'system_log_flushes_total' OR labels['result'] = 'success')
ORDER BY metric;

-- The counters exist from the creation of the system log, also when nothing was counted yet.
SELECT metric, labels['result'], labels['reason'], value >= 0
FROM system.dimensional_metrics
WHERE metric IN ('system_log_flushes_total', 'system_log_dropped_rows_total')
    AND labels['database'] = 'system' AND labels['table'] = 'query_log'
ORDER BY metric, labels['result'], labels['reason'];
