-- With `optimize_move_to_prewhere_if_final_non_intersecting`, the WHERE does not evaluate again the conditions applied early,
-- also when one of them is a column itself, like `flag` in `WHERE flag AND f(v)`, and keeps the values of that column.

DROP TABLE IF EXISTS t_final_prewhere_bare_input;
CREATE TABLE t_final_prewhere_bare_input (k UInt64, flag UInt8, v UInt64)
ENGINE = ReplacingMergeTree ORDER BY k SETTINGS index_granularity = 128;
-- With `optimize_on_insert`, the insert already creates a merged part (level 1).
INSERT INTO t_final_prewhere_bare_input SELECT number, number % 3, number FROM numbers(20000) SETTINGS optimize_on_insert = 1;

-- Compiled expressions would turn the WHERE alone into one function, but not the filter of the read, like PREWHERE.
SET optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1, query_plan_optimize_lazy_final = 0,
    query_plan_optimize_lazy_materialization = 0, use_query_condition_cache = 0, max_threads = 1, max_block_size = 128,
    compile_expressions = 0;

SELECT sum(flag), sum(k) FROM t_final_prewhere_bare_input FINAL WHERE flag AND (v % 5) + (v % 7) != 0
SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0, log_comment = '05482_disabled';
SELECT sum(flag), sum(k) FROM t_final_prewhere_bare_input FINAL WHERE flag AND (v % 5) + (v % 7) != 0
SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1, log_comment = '05482_enabled';

-- The filter is the column itself, which the query also returns.
SELECT sum(flag), sum(k) FROM t_final_prewhere_bare_input FINAL WHERE flag SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 0;
SELECT sum(flag), sum(k) FROM t_final_prewhere_bare_input FINAL WHERE flag SETTINGS optimize_move_to_prewhere_if_final_non_intersecting = 1;

SYSTEM FLUSH LOGS query_log;

-- Evaluating the conditions again would execute about twice as many functions.
SELECT enabled.f * 4 < disabled.f * 5
FROM
(
    SELECT ProfileEvents['FunctionExecute'] AS f FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05482_enabled' AND event_date >= yesterday()
) AS enabled,
(
    SELECT ProfileEvents['FunctionExecute'] AS f FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05482_disabled' AND event_date >= yesterday()
) AS disabled;

DROP TABLE t_final_prewhere_bare_input;
