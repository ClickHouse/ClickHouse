-- The `LIMIT BY` of a query with `ORDER BY` returns its rows in that order also when it spills. Here the
-- expression below `LIMIT BY` drops the last sort expression, so only the first sort column `s` reaches the
-- step. Restoring the order by `s` alone would return the rows with equal `s` in another order, and the outer
-- `LIMIT` would take other rows. Every spilling query returns the rows of the in-memory query before it.
-- The threshold is compared with the memory tracked by the query, so allocations must not linger in the
-- thread-local untracked buffers. At a 1-byte threshold a spilling `LIMIT BY` spills from its first chunk.
SET max_bytes_ratio_before_external_limit_by = 0;
SET max_bytes_before_external_limit_by = 1;
SET max_untracked_memory = 0;
SET max_block_size = 1000;
-- An enabled adaptive memory scheduler selects the spilling transform even without a threshold.
SET enable_adaptive_memory_spill_scheduler = 0;
SET enable_parallel_replicas = 0;

CREATE VIEW sort_suffix_rows AS SELECT number % 2 AS s, number % 1000 AS k, number AS v FROM numbers(20000);

SELECT groupArray((s, k, v)) FROM (SELECT s, k, v FROM sort_suffix_rows ORDER BY s, v + 1 DESC LIMIT 2 BY k LIMIT 3) SETTINGS max_bytes_before_external_limit_by = 0;
SELECT groupArray((s, k, v)) FROM (SELECT s, k, v FROM sort_suffix_rows ORDER BY s, v + 1 DESC LIMIT 2 BY k LIMIT 3) SETTINGS log_comment = '05485_external_limit_by_dropped_sort_suffix/expression';

-- The dropped sort expression orders the rows as strings.
SELECT groupArray((s, k, v)) FROM (SELECT s, k, v FROM sort_suffix_rows ORDER BY s, toString(v) DESC LIMIT 2 BY k LIMIT 3) SETTINGS max_bytes_before_external_limit_by = 0;
SELECT groupArray((s, k, v)) FROM (SELECT s, k, v FROM sort_suffix_rows ORDER BY s, toString(v) DESC LIMIT 2 BY k LIMIT 3) SETTINGS log_comment = '05485_external_limit_by_dropped_sort_suffix/string';

-- The old analyzer records the order of the query in its own planner.
SELECT groupArray((s, k, v)) FROM (SELECT s, k, v FROM sort_suffix_rows ORDER BY s, v + 1 DESC LIMIT 2 BY k LIMIT 3) SETTINGS enable_analyzer = 0, max_bytes_before_external_limit_by = 0;
SELECT groupArray((s, k, v)) FROM (SELECT s, k, v FROM sort_suffix_rows ORDER BY s, v + 1 DESC LIMIT 2 BY k LIMIT 3) SETTINGS enable_analyzer = 0, log_comment = '05485_external_limit_by_dropped_sort_suffix/old_analyzer';

DROP VIEW sort_suffix_rows;

-- Every query above with a `log_comment` spilled and merged its runs.
SYSTEM FLUSH LOGS query_log;
SELECT
    substring(log_comment, length('05485_external_limit_by_dropped_sort_suffix/') + 1) AS test_case,
    min(ProfileEvents['ExternalLimitByWritePart'] > 0) AS spilled,
    min(ProfileEvents['ExternalLimitByMerge'] > 0) AS merged
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND startsWith(log_comment, '05485_external_limit_by_dropped_sort_suffix/')
GROUP BY log_comment
ORDER BY test_case;
