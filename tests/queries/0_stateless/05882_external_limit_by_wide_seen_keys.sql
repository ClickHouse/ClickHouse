-- At its first spill, `LIMIT BY` writes the groups it has seen in batches with a byte target just under
-- 16 MiB. An empty `LowCardinality(FixedString(16777215))` column already exceeds that target, because its
-- dictionary holds the default value of 16777215 bytes, so every batch takes a group before it checks the
-- target. The threshold spills after the first groups, and the query keeps one row per key, as processing in
-- memory does. The threshold is compared with the memory tracked by the query, so allocations must not linger
-- in the thread-local untracked buffers.
SET max_threads = 1, max_block_size = 1, max_untracked_memory = 0;
SET max_bytes_ratio_before_external_limit_by = 0, max_bytes_before_external_limit_by = '256Mi';

SELECT count(), arraySort(groupArray(v)) FROM
(
    SELECT toLowCardinality(toFixedString(toString(number % 2), 16777215)) AS k, number AS v
    FROM numbers(3)
    LIMIT 1 BY k
)
SETTINGS log_comment = '05882_external_limit_by_wide_seen_keys';

-- `system.query_log` also holds the queries of every other test running in parallel, so read it with the
-- default settings.
SET max_untracked_memory = DEFAULT;

-- The query spilled.
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ExternalLimitByWritePart'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment = '05882_external_limit_by_wide_seen_keys';
