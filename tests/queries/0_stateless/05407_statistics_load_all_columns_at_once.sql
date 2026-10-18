-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: the statistics are loaded on the initiator only.

-- The statistics of all columns of a data part are read at once, by the first query that needs the
-- statistics of any column, so a query that needs the statistics of other columns reads nothing from disk.

SET use_statistics = 1;
SET use_statistics_for_part_pruning = 1;
SET materialize_statistics_on_insert = 1;
SET automatic_parallel_replicas_mode = 0;
SET use_query_cache = 0;

DROP TABLE IF EXISTS t_05407;

CREATE TABLE t_05407 (k UInt64, a UInt64, b UInt64, c UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS auto_statistics_types = 'basic', prewarm_statistics_cache = 0;

INSERT INTO t_05407 SELECT number, number, number, number FROM numbers(1000);

-- A new part knows the estimates of its statistics without reading them; a reattached part does not.
DETACH TABLE t_05407;
ATTACH TABLE t_05407;

-- The part is pruned by the statistics of `a`.
SELECT count() FROM t_05407 WHERE a > 10000 SETTINGS log_comment = 'stats_05407_1_column_a';

-- The part is pruned by the statistics of `b` and `c`, which were read together with the statistics of `a`.
SELECT count() FROM t_05407 WHERE b > 10000 OR c > 10000 SETTINGS log_comment = 'stats_05407_2_columns_b_c';

SYSTEM FLUSH LOGS query_log;

-- The first query reads the statistics from disk, unless a concurrent query still holds the detached part,
-- which then keeps its cache entry, so only the second query is checked for reading nothing.
SELECT
    log_comment,
    ProfileEvents['StatisticsCacheHits'] + ProfileEvents['StatisticsCacheMisses'] > 0 AS used_statistics,
    log_comment LIKE '%_1_%' OR ProfileEvents['LoadedStatisticsMicroseconds'] = 0 AS nothing_read_from_disk
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'stats_05407_%'
ORDER BY log_comment;

DROP TABLE t_05407;
