-- With `join_overflow_mode = 'throw'`, a deferred build checks `max_bytes_in_join` after each block against the stored
-- blocks. The streaming build holds the same blocks and its maps on top, so a join that fails this check fails there too.
-- Such a join fails before any key is inserted. The exact check, against what the maps hold, still runs after the
-- inserts. `max_rows_in_join` is checked only there, see 05412.

SET join_algorithm = 'parallel_hash';
SET collect_hash_table_stats_during_joins = 0;
-- With a spill threshold the stored blocks keep the keys, and only then does an `INNER` build defer. This one is far
-- above what these builds take.
SET max_bytes_before_external_join = 10737418240;
SET max_bytes_ratio_before_external_join = 0;
SET max_threads = 4;
SET query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0;
SET join_overflow_mode = 'throw';
SET hash_join_layout = 'parallel';
SET use_statistics = 0; -- Otherwise the planner sizes the builds from the column statistics, and they do not defer.

DROP TABLE IF EXISTS t_r;
CREATE TABLE t_r (k UInt64) ENGINE = MergeTree ORDER BY ();
-- The stored blocks take 8 MB. With its maps, the deferred build takes about 42 MB and the streaming build about 75 MB.
INSERT INTO t_r SELECT number FROM numbers(1000000);

-- 6 MB: the empty maps and the first blocks fit, all the stored blocks do not.
SELECT count() FROM (SELECT 1 AS k) l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_in_join = 6000000, log_comment = 'blocks_over_inner'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM (SELECT 1 AS k) l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, max_bytes_in_join = 6000000; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
-- A `RIGHT` join defers without a spill threshold too.
SELECT count() FROM (SELECT 1 AS k) l RIGHT JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_before_external_join = 0, max_bytes_in_join = 6000000,
    log_comment = 'blocks_over_right'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- 30 MB: the stored blocks fit, the maps do not.
SELECT count() FROM (SELECT 1 AS k) l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_in_join = 30000000, log_comment = 'stored_fits_inner'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SELECT count() FROM (SELECT 1 AS k) l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, max_bytes_in_join = 30000000; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- 100 MB: both builds fit.
SELECT count() FROM (SELECT 1 AS k) l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_in_join = 100000000, log_comment = 'all_fits_inner';
SELECT count() FROM (SELECT 1 AS k) l INNER JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, max_bytes_in_join = 100000000;
SELECT count() FROM (SELECT 1 AS k) l RIGHT JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_before_external_join = 0, max_bytes_in_join = 100000000,
    log_comment = 'all_fits_right';
SELECT count() FROM (SELECT 1 AS k) l RIGHT JOIN t_r r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, max_bytes_before_external_join = 0, max_bytes_in_join = 100000000;

-- Whether each deferred build inserted its keys.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, type, ProfileEvents['HashJoinDeferredBuilds']
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type != 'QueryStart'
    AND log_comment IN ('blocks_over_inner', 'blocks_over_right', 'stored_fits_inner', 'all_fits_inner', 'all_fits_right')
ORDER BY log_comment;

DROP TABLE t_r;
