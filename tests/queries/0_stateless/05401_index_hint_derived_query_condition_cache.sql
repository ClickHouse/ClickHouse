-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: a sibling test's SYSTEM CLEAR QUERY CONDITION CACHE can evict the entry
-- https://github.com/ClickHouse/ClickHouse/issues/124502: the query condition cache tells apart a derived hint bound to different columns

SET optimize_and_compare_chain = 1;
SET query_plan_filter_push_down = 1;
SET use_query_condition_cache = 1;

DROP TABLE IF EXISTS t_hint_qcc;
CREATE TABLE t_hint_qcc (k UInt32, w UInt32, b UInt32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;
INSERT INTO t_hint_qcc SELECT number, 0, 1 FROM numbers(100);
OPTIMIZE TABLE t_hint_qcc FINAL;

-- `a < m AND m < 3` derives `indexHint(a < 3)` below GROUP BY, with `a` bound to `k` here and to `w` below
SELECT countIf(explain LIKE '%Granules: 1/%') > 0 FROM (EXPLAIN indexes = 1 SELECT n FROM (SELECT k AS a, max(b) AS m, count() AS n FROM t_hint_qcc GROUP BY a) WHERE a < m AND m < 3);
SELECT n FROM (SELECT k AS a, max(b) AS m, count() AS n FROM t_hint_qcc GROUP BY a) WHERE a < m AND m < 3 SETTINGS log_comment = '05401_qcc_populate';
SELECT n FROM (SELECT k AS a, max(b) AS m, count() AS n FROM t_hint_qcc GROUP BY a) WHERE a < m AND m < 3 SETTINGS log_comment = '05401_qcc_reuse';
SELECT n FROM (SELECT w AS a, max(b) AS m, count() AS n FROM t_hint_qcc GROUP BY a) WHERE a < m AND m < 3;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryConditionCacheHits'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05401_qcc_reuse'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_hint_qcc;
