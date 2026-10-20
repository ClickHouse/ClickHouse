-- `max_rows_in_join` counts the keys in the maps. A deferred build checks it once, after the inserts: it throws exactly
-- when the streaming build throws, and only after the whole right side has been read.

SET join_algorithm = 'parallel_hash';
SET collect_hash_table_stats_during_joins = 0;
-- With a spill threshold the stored blocks keep the keys, and only then does an `INNER` or `LEFT` build defer. This one
-- is far above what these builds take.
SET max_bytes_before_external_join = 10737418240;
SET max_bytes_ratio_before_external_join = 0;
SET max_threads = 4;
SET query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0;
SET join_overflow_mode = 'throw';
SET parallel_hash_join_threshold = 0;
SET use_statistics = 0; -- Otherwise the planner sizes the builds from the column statistics, and they do not defer.

DROP TABLE IF EXISTS t_r;
CREATE TABLE t_r (k UInt64) ENGINE = MergeTree ORDER BY ();
-- 1000000 distinct keys, each twice.
INSERT INTO t_r SELECT number % 1000000 FROM numbers(2000000);

SELECT count() FROM (SELECT 1 AS k) l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, max_rows_in_join = 1000; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM (SELECT 1 AS k) l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, max_rows_in_join = 999999; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM (SELECT 1 AS k) l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, max_rows_in_join = 999999; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM (SELECT 1 AS k) l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, max_rows_in_join = 1000000;
SELECT count() FROM (SELECT 1 AS k) l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, max_rows_in_join = 1000000;

DROP TABLE t_r;
