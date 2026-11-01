-- The sort below each `LIMIT BY` and `DISTINCT` holds more memory than their 16 MB spill thresholds, while
-- their own state stays far below: the outer `LIMIT` stops each query after its first block. A spill would
-- free nothing and hold back the results until the input ends, so a state below the smaller of the threshold
-- and about 16 MiB keeps hashing.
SET max_bytes_before_external_sort = 0, max_bytes_ratio_before_external_sort = 0;
SET max_bytes_ratio_before_external_limit_by = 0, max_bytes_ratio_before_external_distinct = 0;
SET max_untracked_memory = 0;
SET enable_parallel_replicas = 0;
-- The state of the first block decides each case.
SET max_block_size = 65536;

SELECT count(), sum(v)
FROM (SELECT k, v FROM (SELECT number % 1000000 AS k, number AS v FROM numbers(3000000)) ORDER BY v DESC LIMIT 1 BY k LIMIT 100)
SETTINGS max_bytes_before_external_limit_by = 16000000, log_comment = '05483_external_group_limit_spill_floor/limit_by';

SELECT count()
FROM (SELECT DISTINCT k FROM (SELECT number % 1000000 AS k, number AS v FROM numbers(3000000) ORDER BY v DESC) LIMIT 100)
SETTINGS max_bytes_before_external_distinct = 16000000, query_plan_remove_redundant_sorting = 0,
    log_comment = '05483_external_group_limit_spill_floor/distinct';

-- A threshold below 16 MiB lowers the floor with it, so the state of the first block spills.
SELECT count(), sum(v)
FROM (SELECT k, v FROM (SELECT number % 1000000 AS k, number AS v FROM numbers(3000000)) ORDER BY v DESC LIMIT 1 BY k LIMIT 100)
SETTINGS max_bytes_before_external_limit_by = 100000, log_comment = '05483_external_group_limit_spill_floor/low_threshold';

SYSTEM FLUSH LOGS query_log;
SELECT
    substring(log_comment, length('05483_external_group_limit_spill_floor/') + 1) AS test_case,
    ProfileEvents['ExternalLimitByWritePart'] + ProfileEvents['ExternalDistinctWritePart'] > 0 AS spilled
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND startsWith(log_comment, '05483_external_group_limit_spill_floor/')
ORDER BY test_case;
