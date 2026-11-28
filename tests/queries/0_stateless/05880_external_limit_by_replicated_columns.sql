-- `ARRAY JOIN` repeats the 16 KiB string of its row lazily: a replicated column stores the value once and an
-- index per row. A constant string would not be replicated, so the query materializes it. A spilling
-- `LIMIT BY` keeps that form through its sorting units, runs and merge, so the 65536 rows below, 1 GiB when
-- expanded, fit under a 256 MiB memory limit. The keys are unique, so every row is kept. A one-byte threshold
-- spills from the first block on, and the threshold is compared with the memory tracked by the query, so
-- allocations must not linger in the thread-local untracked buffers.
SET max_threads = 1, max_untracked_memory = 0, enable_lazy_columns_replication = 1, max_memory_usage = '256Mi';
SET max_bytes_ratio_before_external_limit_by = 0, max_bytes_before_external_limit_by = 1;

SELECT count(), sum(length(payload)) FROM
(
    SELECT k, payload FROM (SELECT materialize(repeat('x', 16384)) AS payload, range(65536) AS ks) ARRAY JOIN ks AS k
    LIMIT 1 BY k
)
SETTINGS log_comment = '05880_external_limit_by_replicated_columns';

-- `system.query_log` also holds the queries of every other test running in parallel, so read it with the
-- default settings.
SET max_untracked_memory = DEFAULT, max_memory_usage = DEFAULT;

-- The query spilled.
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ExternalLimitByWritePart'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment = '05880_external_limit_by_replicated_columns';
