-- A `LIMIT BY` above `SHUFFLE` returns the rows in the shuffled order also when it spills. The rows of a
-- query on one server carry no column of this order, so arrival numbers restore it. A spilled `LIMIT BY` that
-- lost the order would return the keys grouped, here in ascending order, which a shuffle of 1000 keys does
-- not produce by chance.
-- At a 1-byte threshold a spilling `LIMIT BY` spills from its first chunk. The threshold is compared with
-- the memory tracked by the query, so allocations must not linger in the thread-local untracked buffers.
SET max_bytes_ratio_before_external_limit_by = 0;
SET max_bytes_before_external_limit_by = 1;
SET max_untracked_memory = 0;
-- An enabled adaptive memory scheduler selects the spilling transform even without a threshold.
SET enable_adaptive_memory_spill_scheduler = 0;
SET enable_parallel_replicas = 0;
SET allow_experimental_shuffle_query = 1;

SELECT length(keys), keys != arraySort(keys)
FROM (SELECT groupArray(k) AS keys FROM (SELECT number % 1000 AS k FROM numbers(10000) SHUFFLE LIMIT 1 BY k))
SETTINGS max_threads = 1, log_comment = '05876_external_limit_by_shuffle_order';

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ExternalLimitByWritePart'] > 0 AS spilled
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment = '05876_external_limit_by_shuffle_order';
