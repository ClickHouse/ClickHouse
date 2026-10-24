-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: the statistics are loaded on the initiator only.

-- The statistics of all columns of a data part are read at once, by the first query that needs the
-- statistics of any column, so a query that needs the statistics of other columns reads nothing from disk.

SET use_statistics = 1;
-- The memoized estimates of a part are used for part pruning without going through the statistics cache,
-- so the statistics are only requested to reorder the PREWHERE conditions.
SET use_statistics_for_part_pruning = 0;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, allow_reorder_prewhere_conditions = 1;
SET materialize_statistics_on_insert = 1;
SET automatic_parallel_replicas_mode = 0;
SET use_query_cache = 0;

DROP TABLE IF EXISTS t_05407;

CREATE TABLE t_05407 (k UInt64, a UInt64, b UInt64, c UInt64, d UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS auto_statistics_types = 'basic', prewarm_statistics_cache = 0;

INSERT INTO t_05407 SELECT number, number, number, number, number FROM numbers(1000);

-- A reattached part has nothing in the statistics cache.
DETACH TABLE t_05407;
ATTACH TABLE t_05407;

-- Needs the statistics of `a` and `b`.
SELECT count() FROM t_05407 WHERE a < 500 AND b < 900 SETTINGS log_comment = 'stats_05407_1_columns_a_b';

-- Needs the statistics of `c` and `d`, which were read together with the statistics of `a` and `b`.
SELECT count() FROM t_05407 WHERE c < 500 AND d < 900 SETTINGS log_comment = 'stats_05407_2_columns_c_d';

SYSTEM FLUSH LOGS query_log;

-- The first query may find the statistics in the cache if a concurrent query of `system.parts_columns`
-- has loaded them, so it is only checked for using the cache.
SELECT
    log_comment,
    ProfileEvents['StatisticsCacheHits'] + ProfileEvents['StatisticsCacheMisses'] > 0 AS used_statistics,
    log_comment LIKE '%_1_%' OR (ProfileEvents['StatisticsCacheMisses'] = 0 AND ProfileEvents['LoadedStatisticsMicroseconds'] = 0) AS nothing_read_from_disk
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'stats_05407_%'
ORDER BY log_comment;

DROP TABLE t_05407;
