-- With `optimize_move_to_prewhere_if_final_non_intersecting`, the conditions of a conjunction that the query returns, and that
-- is a part of the `WHERE`, are not applied early: the query would evaluate them again to return the conjunction.

DROP TABLE IF EXISTS t_final_prewhere_sub_conjunction;
CREATE TABLE t_final_prewhere_sub_conjunction (k UInt64, v UInt64)
ENGINE = ReplacingMergeTree ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO t_final_prewhere_sub_conjunction SELECT number, number FROM numbers(20000);
OPTIMIZE TABLE t_final_prewhere_sub_conjunction FINAL;

SET optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1, query_plan_optimize_lazy_final = 0,
    query_plan_optimize_lazy_materialization = 0, use_query_condition_cache = 0, max_threads = 1, max_block_size = 128;

SELECT sum(f), sum(k) FROM
(
    SELECT k, (v % 3 != 0 AND v % 5 != 0 AND v % 7 != 0) AS f FROM t_final_prewhere_sub_conjunction FINAL WHERE f AND v % 11 != 0
)
SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0, log_comment = '05481_disabled';
SELECT sum(f), sum(k) FROM
(
    SELECT k, (v % 3 != 0 AND v % 5 != 0 AND v % 7 != 0) AS f FROM t_final_prewhere_sub_conjunction FINAL WHERE f AND v % 11 != 0
)
SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1, log_comment = '05481_enabled';

SYSTEM FLUSH LOGS query_log;

-- Evaluating the conditions of the conjunction again would execute about 1.5 times as many functions.
SELECT enabled.f * 4 < disabled.f * 5
FROM
(
    SELECT ProfileEvents['FunctionExecute'] AS f FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05481_enabled' AND event_date >= yesterday()
) AS enabled,
(
    SELECT ProfileEvents['FunctionExecute'] AS f FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05481_disabled' AND event_date >= yesterday()
) AS disabled;

DROP TABLE t_final_prewhere_sub_conjunction;
