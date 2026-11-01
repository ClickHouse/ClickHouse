-- With `optimize_move_to_prewhere_if_final_non_intersecting`, the `WHERE` does not evaluate again the conditions that the
-- read of the non-intersecting ranges of FINAL applied, also when the query returns the conjunction of the conditions.

DROP TABLE IF EXISTS t_final_prewhere_kept;
CREATE TABLE t_final_prewhere_kept (k UInt64, v UInt64, n Nullable(UInt64))
ENGINE = ReplacingMergeTree ORDER BY k SETTINGS index_granularity = 128;
-- With `optimize_on_insert`, the insert already creates a merged part (level 1).
INSERT INTO t_final_prewhere_kept SELECT number, number, number FROM numbers(20000) SETTINGS optimize_on_insert = 1;

SET optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1, query_plan_optimize_lazy_final = 0,
    query_plan_optimize_lazy_materialization = 0, use_query_condition_cache = 0, max_threads = 1, max_block_size = 128;

SELECT sum(f), sum(k), any(toTypeName(f)) FROM
(
    SELECT k, (v % 3 != 0 AND v % 5 != 0 AND v % 7 != 0 AND v % 11 != 0) AS f FROM t_final_prewhere_kept FINAL WHERE f
)
SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT sum(f), sum(k), any(toTypeName(f)) FROM
(
    SELECT k, (v % 3 != 0 AND v % 5 != 0 AND v % 7 != 0 AND v % 11 != 0) AS f FROM t_final_prewhere_kept FINAL WHERE f
)
SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1, log_comment = '05480_kept';

-- The same conditions, without returning their conjunction.
SELECT sum(k) FROM t_final_prewhere_kept FINAL WHERE v % 3 != 0 AND v % 5 != 0 AND v % 7 != 0 AND v % 11 != 0
SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1, log_comment = '05480_removed';

-- A `Nullable` conjunction.
SELECT sum(f), sum(k), any(toTypeName(f)) FROM
(
    SELECT k, (n % 3 != 0 AND v % 5 != 0) AS f FROM t_final_prewhere_kept FINAL WHERE f
)
SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;

SYSTEM FLUSH LOGS query_log;

-- Evaluating the conditions again would almost double the number of executed functions.
SELECT kept.f * 4 < removed.f * 5
FROM
(
    SELECT ProfileEvents['FunctionExecute'] AS f FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05480_kept' AND event_date >= yesterday()
) AS kept,
(
    SELECT ProfileEvents['FunctionExecute'] AS f FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05480_removed' AND event_date >= yesterday()
) AS removed;

DROP TABLE t_final_prewhere_kept;

-- A kept condition of the sorting key that is a constant, read from merged and non-intersecting ranges:
-- the non-intersecting ranges must return it as a full column, like PREWHERE on the merged ones.
DROP TABLE IF EXISTS t_final_prewhere_kept_const;
CREATE TABLE t_final_prewhere_kept_const (k UInt64, v UInt64)
ENGINE = ReplacingMergeTree ORDER BY k SETTINGS index_granularity = 32;
INSERT INTO t_final_prewhere_kept_const SELECT number, number % 4 FROM numbers(256) SETTINGS optimize_on_insert = 1;
SYSTEM STOP MERGES t_final_prewhere_kept_const;
INSERT INTO t_final_prewhere_kept_const SELECT number, number % 5 FROM numbers(80, 60);

SELECT count(), sum(k) FROM t_final_prewhere_kept_const FINAL WHERE v = 2 AND ignore(k) = 0
SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0, max_threads = 2;
SELECT count(), sum(k) FROM t_final_prewhere_kept_const FINAL WHERE v = 2 AND ignore(k) = 0
SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1, max_threads = 2;

DROP TABLE t_final_prewhere_kept_const;
