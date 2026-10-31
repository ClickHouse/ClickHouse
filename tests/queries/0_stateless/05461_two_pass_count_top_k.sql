-- Tags: no-parallel-replicas
-- no-parallel-replicas: the optimization does not apply with parallel replicas.

SET query_plan_two_pass_count_top_k = 1, query_plan_two_pass_count_top_k_min_rows = 0;
SET enable_parallel_replicas = 0, use_query_condition_cache = 0, optimize_aggregation_in_order = 0, serialize_query_plan = 0;
-- Read, sort and speed limits disable the optimization: the test profile sets them all.
SET max_rows_to_read = 0, max_bytes_to_read = 0, max_bytes_to_read_leaf = 0, max_rows_to_group_by = 0, max_rows_to_sort = 0,
    max_bytes_to_sort = 0, max_execution_speed = 0, max_execution_speed_bytes = 0, max_estimated_execution_time = 0;

DROP TABLE IF EXISTS t_two_pass;
CREATE TABLE t_two_pass (k UInt64, s String, m UInt8, x Int32, u UInt64, ks Int32) ENGINE = MergeTree ORDER BY (m, k)
SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

-- 10% of the rows in 20 hot keys with 5 strings each, 30% in 3000 warm keys, 60% scattered over 300000 keys; `u` is unique.
INSERT INTO t_two_pass SELECT
    multiIf(number % 10 = 0, intHash64(number) % 20, number % 10 < 4, 1000 + intHash64(number) % 3000, 100000 + intHash64(number) % 300000) AS k,
    multiIf(number % 10 = 0, toString(intHash32(number) % 5), number % 10 < 4, toString(intHash32(number) % 50), toString(intHash32(number) % 1000)) AS s,
    number % 4 AS m, toInt32(number % 1000) AS x, number AS u, toInt32(k) - 1000 AS ks
FROM numbers(1000000);

SELECT 'results with and without the optimization';
SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC, k, s LIMIT 10 SETTINGS log_comment = 'two_pass_q1_on';
SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC, k, s LIMIT 10 SETTINGS log_comment = 'two_pass_q1_off', query_plan_two_pass_count_top_k = 0;

-- WHERE moved to PREWHERE, another aggregate, an alias of count().
SELECT k, s, count() AS c, sum(x) FROM t_two_pass WHERE m != 1 GROUP BY k, s ORDER BY c DESC, k, s LIMIT 7 SETTINGS log_comment = 'two_pass_q3_on';

-- The string key first, a computed third key, an explicit PREWHERE, a signed 32-bit bounding key.
SELECT s, ks, x % 3 AS r, count() FROM t_two_pass PREWHERE x < 900 GROUP BY s, ks, r ORDER BY count() DESC, s, ks, r LIMIT 6 OFFSET 2 SETTINGS log_comment = 'two_pass_q4_on';

-- Aliases with the names of the nodes the passes add do not name nodes of the plan.
SELECT k AS __two_pass_count_top_k_bound, s, count() AS __two_pass_count_top_k_in FROM t_two_pass GROUP BY __two_pass_count_top_k_bound, s
ORDER BY __two_pass_count_top_k_in DESC, __two_pass_count_top_k_bound, s LIMIT 5 SETTINGS log_comment = 'two_pass_q7_on';

SELECT 'rows_before_limit_at_least';
SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC, k, s LIMIT 3
SETTINGS log_comment = 'two_pass_rows_before_limit_on', log_processors_profiles = 1, output_format_write_statistics = 0 FORMAT JSONCompact;

SYSTEM FLUSH LOGS query_log;

SELECT 'passes, fallbacks, all rows of every pass in read_rows';
SELECT log_comment, ProfileEvents['TwoPassCountTopKPasses'] >= 3, ProfileEvents['TwoPassCountTopKFallbacks'], read_rows > 1100000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'two\_pass\_%'
ORDER BY log_comment;

SELECT 'the processors of the passes belong to the step';
SYSTEM FLUSH LOGS processors_profile_log;
SELECT countIf(plan_step_name = 'TwoPassCountTopK') > 10, countIf(plan_step_name = 'TwoPassCountTopK' AND name LIKE 'MergeTreeSelect%') > 0
FROM system.processors_profile_log
WHERE event_date >= yesterday() AND query_id = (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'two_pass_rows_before_limit_on' AND event_date >= yesterday()
    ORDER BY event_time_microseconds DESC LIMIT 1);

DROP TABLE t_two_pass;
