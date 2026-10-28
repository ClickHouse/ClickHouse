-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-random-settings: read limits and overflow modes disable the optimization, and the pilot depends on the granules.

SET query_plan_two_pass_count_top_k = 1, query_plan_two_pass_count_top_k_min_rows = 0;
SET enable_parallel_replicas = 0, use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_two_pass;
CREATE TABLE t_two_pass (k UInt64, s String, m UInt8, x Int32, u UInt64, ks Int32) ENGINE = MergeTree ORDER BY (m, k)
SETTINGS index_granularity = 128, index_granularity_bytes = 0;

-- 10% of the rows in 20 hot keys with 5 strings each, 30% in 3000 warm keys, 60% scattered over 300000 keys; `u` is unique.
INSERT INTO t_two_pass SELECT
    multiIf(number % 10 = 0, intHash64(number) % 20, number % 10 < 4, 1000 + intHash64(number) % 3000, 100000 + intHash64(number) % 300000) AS k,
    multiIf(number % 10 = 0, toString(intHash32(number) % 5), number % 10 < 4, toString(intHash32(number) % 50), toString(intHash32(number) % 1000)) AS s,
    number % 4 AS m, toInt32(number % 1000) AS x, number AS u, toInt32(k) - 1000 AS ks
FROM numbers(1000000);

-- Many keys with about the same count, each over many strings, and 10 groups of 800 rows among them: the restricted pass
-- over the most frequent keys is not certified, and the final pass runs over all keys frequent enough.
DROP TABLE IF EXISTS t_two_pass_flat;
CREATE TABLE t_two_pass_flat (k UInt32, s String) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 128, index_granularity_bytes = 0;
INSERT INTO t_two_pass_flat SELECT
    if(number % 100 = 0, 37 * (intDiv(number, 100) % 10) + 3, number % 400) AS k,
    if(number % 100 = 0, 'heavy', toString(intHash32(number) % 1000)) AS s
FROM numbers(800000);

SELECT 'results with and without the optimization';
SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC, k, s LIMIT 10 SETTINGS log_comment = 'two_pass_q1_on';
SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC, k, s LIMIT 10 SETTINGS log_comment = 'two_pass_q1_off', query_plan_two_pass_count_top_k = 0;

-- OFFSET, and ties at the boundary broken by the other sort columns.
SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC, k, s LIMIT 10 OFFSET 95 SETTINGS log_comment = 'two_pass_q2_on';
SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC, k, s LIMIT 10 OFFSET 95 SETTINGS query_plan_two_pass_count_top_k = 0;

-- WHERE moved to PREWHERE, another aggregate, an alias of count().
SELECT k, s, count() AS c, sum(x) FROM t_two_pass WHERE m != 1 GROUP BY k, s ORDER BY c DESC, k, s LIMIT 7 SETTINGS log_comment = 'two_pass_q3_on';
SELECT k, s, count() AS c, sum(x) FROM t_two_pass WHERE m != 1 GROUP BY k, s ORDER BY c DESC, k, s LIMIT 7 SETTINGS query_plan_two_pass_count_top_k = 0;

-- The string key first, a computed third key, an explicit PREWHERE, a signed 32-bit bounding key.
SELECT s, ks, x % 3 AS r, count() FROM t_two_pass PREWHERE x < 900 GROUP BY s, ks, r ORDER BY count() DESC, s, ks, r LIMIT 6 OFFSET 2 SETTINGS log_comment = 'two_pass_q4_on';
SELECT s, ks, x % 3 AS r, count() FROM t_two_pass PREWHERE x < 900 GROUP BY s, ks, r ORDER BY count() DESC, s, ks, r LIMIT 6 OFFSET 2 SETTINGS query_plan_two_pass_count_top_k = 0;

-- Not certified: the final pass.
SELECT k, s, count() FROM t_two_pass_flat GROUP BY k, s ORDER BY count() DESC, k, s LIMIT 10 SETTINGS log_comment = 'two_pass_q5_on';
SELECT k, s, count() FROM t_two_pass_flat GROUP BY k, s ORDER BY count() DESC, k, s LIMIT 10 SETTINGS query_plan_two_pass_count_top_k = 0;

-- Only the counts are determined when the boundary has ties and there is no other sort column.
SELECT arraySort(groupArray(c)) FROM (SELECT k, s, count() AS c FROM t_two_pass GROUP BY k, s ORDER BY c DESC LIMIT 50) SETTINGS log_comment = 'two_pass_q6_on';
SELECT arraySort(groupArray(c)) FROM (SELECT k, s, count() AS c FROM t_two_pass GROUP BY k, s ORDER BY c DESC LIMIT 50) SETTINGS query_plan_two_pass_count_top_k = 0;

-- The pilot rejects a near-unique key and small groups: the original plan runs.
SELECT u, s, count() FROM t_two_pass GROUP BY u, s ORDER BY count() DESC, u, s LIMIT 3 SETTINGS log_comment = 'two_pass_unique_on';
SELECT u, s, count() FROM t_two_pass GROUP BY u, s ORDER BY count() DESC, u, s LIMIT 3 SETTINGS query_plan_two_pass_count_top_k = 0;
SELECT k, substring(toString(u), 2) AS su, count() FROM t_two_pass GROUP BY k, su ORDER BY count() DESC, k, su LIMIT 3 SETTINGS log_comment = 'two_pass_small_on';
SELECT k, substring(toString(u), 2) AS su, count() FROM t_two_pass GROUP BY k, su ORDER BY count() DESC, k, su LIMIT 3 SETTINGS query_plan_two_pass_count_top_k = 0;

-- A key outside the primary key: every pass reads all granules, so the restricted pass covers the most frequent keys holding
-- up to a tenth of the rows. The 10 largest groups (1000 rows) belong to keys ranked below 150 keys of 1500 rows over many
-- strings; the restricted pass over 460 keys is the result, without another pass.
DROP TABLE IF EXISTS t_two_pass_unpruned;
CREATE TABLE t_two_pass_unpruned (k UInt32, s String) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 128, index_granularity_bytes = 0;
INSERT INTO t_two_pass_unpruned SELECT
    multiIf(r < 225000, intDiv(r, 1500), r < 237000, 1000 + intDiv(r - 225000, 1200), r < 417000, 2000 + intDiv(r - 237000, 600), 10000 + intDiv(r - 417000, 300)) AS k,
    if(r >= 225000 AND r < 237000 AND (r - 225000) % 1200 < 1000, 'top', toString(intHash32(number) % 1000)) AS s
FROM (SELECT number, (number * 1000003) % 2817000 AS r FROM numbers(2817000));
SELECT k, s, count() FROM t_two_pass_unpruned GROUP BY k, s ORDER BY count() DESC, k, s LIMIT 10 SETTINGS log_comment = 'two_pass_unpruned_on';
SELECT k, s, count() FROM t_two_pass_unpruned GROUP BY k, s ORDER BY count() DESC, k, s LIMIT 10 SETTINGS query_plan_two_pass_count_top_k = 0;

-- An error in a pass is the error of the query.
SELECT k, s, count() FROM t_two_pass WHERE throwIf(u = 777777) = 0 GROUP BY k, s ORDER BY count() DESC LIMIT 10; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
-- Also after the pilot rejects the key: the original plan raises it.
SELECT u, s, count() FROM t_two_pass WHERE throwIf(u = 777777) = 0 GROUP BY u, s ORDER BY count() DESC, u, s LIMIT 3 SETTINGS log_comment = 'two_pass_unique_error'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

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
-- Settings under which a pass could return a different partial result or count the rows of every pass against a limit.
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS max_rows_to_read = 100000000;
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS timeout_overflow_mode = 'break';
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS max_rows_to_group_by = 100000000;
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS query_plan_two_pass_count_top_k = 0;
SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (EXPLAIN SELECT k, s, count() FROM t_two_pass GROUP BY k, s ORDER BY count() DESC LIMIT 10) SETTINGS query_plan_two_pass_count_top_k_min_rows = 10000000;

SYSTEM FLUSH LOGS query_log;

SELECT 'passes, fallbacks, all rows of every pass in read_rows';
SELECT log_comment, ProfileEvents['TwoPassCountTopKPasses'] >= 3, ProfileEvents['TwoPassCountTopKFallbacks'], read_rows > 1000000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'two\_pass\_q%'
ORDER BY log_comment;

SELECT log_comment, ProfileEvents['TwoPassCountTopKPasses'], ProfileEvents['TwoPassCountTopKFallbacks'], read_rows >= 1000000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('two_pass_unique_on', 'two_pass_small_on', 'two_pass_unpruned_on', 'two_pass_explain_pipeline')
ORDER BY log_comment;

SELECT log_comment, ProfileEvents['TwoPassCountTopKPasses'], ProfileEvents['TwoPassCountTopKFallbacks']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'ExceptionWhileProcessing' AND log_comment = 'two_pass_unique_error';

DROP TABLE t_two_pass;
DROP TABLE t_two_pass_flat;
DROP TABLE t_two_pass_unpruned;
