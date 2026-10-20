-- Tags: no-random-settings, no-random-merge-tree-settings
-- The checks depend on the statistics of the tables and on the join order, which randomized settings change.

-- The `uniq` statistic of a column counts the distinct keys of a build only when the key is that column of the whole
-- table. A function of it, a cast that narrows it, a filter, a limit or an aggregation in between leave the build
-- without a known size, and it defers. A subquery that only selects the column keeps the statistic.

SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET join_algorithm = 'parallel_hash';
SET parallel_hash_join_threshold = 0;
SET collect_hash_table_stats_during_joins = 0;
-- With a spill threshold the stored blocks keep the keys, and only then does an `INNER` or `LEFT` build defer. This one
-- is far above what these builds take.
SET max_bytes_before_external_join = 10737418240;
SET max_bytes_ratio_before_external_join = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'false';
SET max_threads = 4;
SET enable_parallel_replicas = 0;
SET enable_hash_join_deferred_build = 1;

CREATE TABLE t1 (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2 (k UInt64 STATISTICS(uniq), ts DateTime STATISTICS(uniq), v UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t1 SELECT number FROM numbers(100000);
INSERT INTO t2 SELECT number % 30000, toDateTime('2000-01-01 00:00:00') + number * 3600, number FROM numbers(300000);

SELECT 'plain', count() FROM t1 INNER JOIN t2 ON t1.k = t2.k SETTINGS log_comment = '05418 plain';
SELECT 'subquery', count() FROM t1 INNER JOIN (SELECT k, v FROM t2) AS r ON t1.k = r.k SETTINGS log_comment = '05418 subquery';

SELECT 'toYear', count() FROM t1 INNER JOIN t2 ON t1.k = toYear(t2.ts) SETTINGS log_comment = '05418 neg_toYear';
SELECT 'intDiv', count() FROM t1 INNER JOIN t2 ON t1.k = intDiv(t2.k, 10) SETTINGS log_comment = '05418 neg_intDiv';
SELECT 'cast', count() FROM t1 INNER JOIN t2 ON t1.k = CAST(t2.k AS UInt8) SETTINGS log_comment = '05418 neg_cast';
SELECT 'plus', count() FROM t1 INNER JOIN t2 ON t1.k = t2.k + 1 SETTINGS log_comment = '05418 neg_plus';
SELECT 'filter', count() FROM t1 INNER JOIN t2 ON t1.k = t2.k WHERE t2.v % 2 = 0 SETTINGS log_comment = '05418 neg_filter';
SELECT 'limit', count() FROM t1 INNER JOIN (SELECT k FROM t2 LIMIT 1000) AS r ON t1.k = r.k SETTINGS log_comment = '05418 neg_limit';
SELECT 'group_by', count() FROM t1 INNER JOIN (SELECT k FROM t2 GROUP BY k) AS r ON t1.k = r.k SETTINGS log_comment = '05418 neg_group_by';
SELECT 'union', count() FROM t1 INNER JOIN (SELECT k FROM t2 UNION ALL SELECT k FROM t2) AS r ON t1.k = r.k SETTINGS log_comment = '05418 neg_union';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['HashJoinPreallocatedElementsInHashTables'] > 0,
    ProfileEvents['HashJoinDeferredBuilds']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05418 %'
ORDER BY log_comment;
