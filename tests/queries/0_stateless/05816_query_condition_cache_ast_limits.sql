-- Tags: no-parallel
-- Tag no-parallel: messes with the query condition cache

-- `parseQueryToJSON` and `formatQueryFromJSON` snapshot `max_ast_depth`, `max_ast_elements` and
-- `allow_settings_after_format_in_insert` when they are built. A lenient session must not prime a "no marks match"
-- verdict that a strict session is then served instead of its exception.

SET use_query_condition_cache = 1;
SET parallel_replicas_local_plan = 1;
-- A positive `parallel_replicas_min_number_of_rows_per_replica` makes the planner estimate the rows to read first,
-- and that estimate consults the query condition cache too, which changes the profile event counters.
SET parallel_replicas_min_number_of_rows_per_replica = 0;

DROP TABLE IF EXISTS t_qcc_ast_limits;
CREATE TABLE t_qcc_ast_limits (k UInt64, q String, j String, i String) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 128, add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
INSERT INTO t_qcc_ast_limits
SELECT number, concat('SELECT ', repeat('plus(1,', 30), '1', repeat(')', 30)), parseQueryToJSON(concat('SELECT 1', repeat(', 1', 30))),
    'INSERT INTO t FORMAT Values SETTINGS max_threads = 1'
FROM numbers(256);

SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_ast_limits WHERE parseQueryToJSON(q) = '' SETTINGS max_ast_depth = 100;
SELECT count() FROM t_qcc_ast_limits WHERE parseQueryToJSON(q) = '' SETTINGS max_ast_depth = 20; -- { serverError TOO_DEEP_AST }

SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_ast_limits WHERE parseQueryToJSON(q) = '' SETTINGS max_ast_elements = 1000;
SELECT count() FROM t_qcc_ast_limits WHERE parseQueryToJSON(q) = '' SETTINGS max_ast_elements = 20; -- { serverError TOO_BIG_AST }

-- `j` holds a flat query: deep JSON overflows the small stack budget of sanitizer builds.
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_ast_limits WHERE formatQueryFromJSON(j) = '' SETTINGS max_ast_elements = 1000;
SELECT count() FROM t_qcc_ast_limits WHERE formatQueryFromJSON(j) = '' SETTINGS max_ast_elements = 20; -- { serverError TOO_BIG_AST }

SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_ast_limits WHERE parseQueryToJSON(i) = '' SETTINGS allow_settings_after_format_in_insert = 1;
SELECT count() FROM t_qcc_ast_limits WHERE parseQueryToJSON(i) = '' SETTINGS allow_settings_after_format_in_insert = 0; -- { serverError BAD_ARGUMENTS }

-- A repeated query with the same settings still reads the cached verdict.
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_ast_limits WHERE parseQueryToJSON(q) = '' SETTINGS max_ast_depth = 100 FORMAT Null;
SELECT count() FROM t_qcc_ast_limits WHERE parseQueryToJSON(q) = '' SETTINGS max_ast_depth = 100 FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryConditionCacheHits'], read_rows
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE '%parseQueryToJSON(q) = '''' SETTINGS max_ast_depth = 100 FORMAT Null%' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_qcc_ast_limits;
