-- Tags: no-parallel-replicas
-- no-parallel-replicas: the optimization does not apply with parallel replicas.

-- Where `query_plan_two_pass_count_top_k` applies, how it falls back to the original plan, and how errors of a pass surface.
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

SELECT 'the pilot rejects a near-unique key and small groups: the original plan runs';
SELECT u, s, count() FROM t_two_pass GROUP BY u, s ORDER BY count() DESC, u, s LIMIT 3 SETTINGS log_comment = 'two_pass_unique_on';
SELECT k, substring(toString(u), 2) AS su, count() FROM t_two_pass GROUP BY k, su ORDER BY count() DESC, k, su LIMIT 3 SETTINGS log_comment = 'two_pass_small_on';

-- An error in a pass is the error of the query.
SELECT k, s, count() FROM t_two_pass WHERE throwIf(u = 777777) = 0 GROUP BY k, s ORDER BY count() DESC LIMIT 10; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
-- Also after the pilot rejects the key: the original plan raises it.
SELECT u, s, count() FROM t_two_pass WHERE throwIf(u = 777777) = 0 GROUP BY u, s ORDER BY count() DESC, u, s LIMIT 3 SETTINGS log_comment = 'two_pass_unique_error'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT 'EXPLAIN ANALYZE';
SELECT countIf(explain LIKE '%TwoPassCountTopK%') > 0 FROM (EXPLAIN ANALYZE SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC, k, s LIMIT 3);

SELECT 'plan';
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass WHERE throwIf(u = 777777) = 0 GROUP BY k, s ORDER BY count() DESC LIMIT 10);
-- EXPLAIN PIPELINE shows the processor and runs no pass.
SELECT countIf(explain LIKE '%TwoPassCountTopKTransform%') FROM (EXPLAIN PIPELINE SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS log_comment = 'two_pass_explain_pipeline';

SELECT 'not applied';
-- Expressions that can change between passes over different granules and blocks: block-dependent, stateful, non-deterministic, also in a lambda.
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, intDiv(rowNumberInAllBlocks(), 100000) AS g, count() FROM t_two_pass GROUP BY k, s, g ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, blockSize() AS g, count() FROM t_two_pass GROUP BY k, s, g ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, arrayMap(x -> rowNumberInAllBlocks(), [s]) AS g, count() FROM t_two_pass GROUP BY k, g ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, arrayMap(x -> blockSize(), [s]) AS g, count() FROM t_two_pass GROUP BY k, g ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass WHERE rand() % 2 = 0 GROUP BY k, s ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count(), sum(rowNumberInAllBlocks()) FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() AS c, blockSize() AS b FROM t_two_pass GROUP BY k, s ORDER BY c DESC, b LIMIT 10);
-- Other shapes.
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s HAVING count() > 1 ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s WITH TOTALS ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 1001);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() * 2 DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10 WITH TIES);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, count() FROM t_two_pass GROUP BY k ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, x, count() FROM t_two_pass GROUP BY k, x ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass WHERE k IN (SELECT k FROM t_two_pass WHERE m = 1) GROUP BY k, s ORDER BY count() DESC LIMIT 10);
-- A function with observable side effects.
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass WHERE sleepEachRow(0) = 0 GROUP BY k, s ORDER BY count() DESC LIMIT 10);
-- A column with the name of a node the passes add.
DROP TABLE IF EXISTS t_two_pass_names;
CREATE TABLE t_two_pass_names (k UInt64, s String, __two_pass_count_top_k_in UInt8) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_two_pass_names SELECT number % 100, toString(number % 7), number % 2 FROM numbers(10000);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass_names PREWHERE __two_pass_count_top_k_in = 1 GROUP BY k, s ORDER BY count() DESC LIMIT 10);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass_names PREWHERE k > 1 GROUP BY k, s ORDER BY count() DESC LIMIT 10);
DROP TABLE t_two_pass_names;
-- Settings under which a pass could return a different partial result or count the rows of every pass against a limit.
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS max_rows_to_read = 100000000;
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS timeout_overflow_mode = 'break';
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS max_rows_to_group_by = 100000000;
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS query_plan_two_pass_count_top_k = 0;
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS query_plan_two_pass_count_top_k_min_rows = 10000000;
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS query_plan_enable_optimizations = 0;
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS serialize_query_plan = 1;
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS rows_before_aggregation = 1;
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS max_rows_to_sort = 1000000000;
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS max_bytes_to_sort = 1000000000000;
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS max_execution_speed = 1000000000;
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS min_execution_speed_bytes = 1;
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS max_estimated_execution_time = 1000;
-- A read limit of an outer query applies to the read of the subquery.
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT * FROM (SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10 SETTINGS max_rows_to_read = 0) SETTINGS max_rows_to_read = 1000000000);
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT * FROM (SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10 SETTINGS max_rows_to_read = 0));

SYSTEM FLUSH LOGS query_log;

SELECT 'passes and fallbacks';
SELECT log_comment, ProfileEvents['TwoPassCountTopKPasses'], ProfileEvents['TwoPassCountTopKFallbacks'], read_rows >= 1000000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('two_pass_unique_on', 'two_pass_small_on', 'two_pass_explain_pipeline')
ORDER BY log_comment;

SELECT log_comment, ProfileEvents['TwoPassCountTopKPasses'], ProfileEvents['TwoPassCountTopKFallbacks']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'ExceptionWhileProcessing' AND log_comment = 'two_pass_unique_error';

DROP TABLE t_two_pass;
