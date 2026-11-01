-- With `optimize_move_to_prewhere_if_final_non_intersecting`, the `WHERE` does not evaluate again the conditions that the
-- read of the non-intersecting ranges of FINAL applied. Lazy materialization (`ORDER BY ... LIMIT`) rebuilds the filter,
-- and the rebuilt filter must still skip them: otherwise the setting evaluates the conditions twice for these rows.

DROP TABLE IF EXISTS t_final_prewhere_lazy;
CREATE TABLE t_final_prewhere_lazy (key UInt64, v UInt64, payload String)
ENGINE = ReplacingMergeTree ORDER BY key SETTINGS index_granularity = 128;
-- With `optimize_on_insert`, the insert already creates a merged part (level 1).
INSERT INTO t_final_prewhere_lazy SELECT number, number, toString(number) FROM numbers(20000) SETTINGS optimize_on_insert = 1;

SET optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1, query_plan_optimize_lazy_final = 0,
    query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10, use_query_condition_cache = 0,
    max_threads = 1, max_block_size = 128, query_plan_merge_filters = 1;

-- Each condition takes several functions, so a second evaluation of them doubles the number of executed functions.
-- Without `query_plan_merge_filters`, the `WHERE` evaluates them in one short-circuit `FilterTransform` with fewer calls.
SELECT payload FROM t_final_prewhere_lazy FINAL
WHERE v % 3 != 0 AND v % 5 != 0 AND v % 7 != 0 AND v % 11 != 0 ORDER BY v DESC LIMIT 3
SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0, log_comment = '05479_off';
SELECT payload FROM t_final_prewhere_lazy FINAL
WHERE v % 3 != 0 AND v % 5 != 0 AND v % 7 != 0 AND v % 11 != 0 ORDER BY v DESC LIMIT 3
SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1, log_comment = '05479_on';

SYSTEM FLUSH LOGS query_log;

SELECT on.f * 4 < off.f * 5
FROM
(
    SELECT ProfileEvents['FunctionExecute'] AS f FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05479_on' AND event_date >= yesterday()
) AS on,
(
    SELECT ProfileEvents['FunctionExecute'] AS f FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05479_off' AND event_date >= yesterday()
) AS off;

DROP TABLE t_final_prewhere_lazy;
