-- Tags: no-parallel-replicas
-- no-parallel-replicas: the optimization does not apply with parallel replicas.

-- The passes `query_plan_two_pass_count_top_k` runs when the restricted pass is not certified: the final pass and the count
-- pass again.
SET query_plan_two_pass_count_top_k = 1, query_plan_two_pass_count_top_k_min_rows = 0;
SET enable_parallel_replicas = 0, use_query_condition_cache = 0, optimize_aggregation_in_order = 0, serialize_query_plan = 0;
-- Read, sort and speed limits disable the optimization: the test profile sets them all.
SET max_rows_to_read = 0, max_bytes_to_read = 0, max_bytes_to_read_leaf = 0, max_rows_to_group_by = 0, max_rows_to_sort = 0,
    max_bytes_to_sort = 0, max_execution_speed = 0, max_execution_speed_bytes = 0, max_estimated_execution_time = 0;

-- Many keys with about the same count, each over many strings, and 10 groups of 800 rows among them: the restricted pass
-- over the most frequent keys is not certified, and the final pass runs over all keys frequent enough.
DROP TABLE IF EXISTS t_two_pass_flat;
CREATE TABLE t_two_pass_flat (k UInt32, s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_two_pass_flat SELECT
    if(number % 100 = 0, 37 * (intDiv(number, 100) % 10) + 3, intHash32(number) % 400) AS k,
    if(number % 100 = 0, 'heavy', toString(intHash32(number) % 1000)) AS s
FROM numbers(800000);

-- 200 groups of exactly 1250 rows (50 keys x 4 strings) spread evenly over a table without a sorting key, the other rows
-- scattered over 200000 keys: the pilot samples every group, so OFFSET, ties and the count-only sort run the passes.
DROP TABLE IF EXISTS t_two_pass_uniform;
CREATE TABLE t_two_pass_uniform (k UInt32, s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_two_pass_uniform SELECT
    if(number % 4 = 0, intDiv(number, 4) % 50, 1000 + intHash32(number) % 200000) AS k,
    if(number % 4 = 0, toString(intDiv(intDiv(number, 4), 50) % 4), toString(intHash32(number + 1) % 1000)) AS s
FROM numbers(1000000);

-- 400 keys of exactly 1000 rows, one group each, the key in the partition key: the count pass fetches 200 of the tied
-- keys, the restricted pass over them is not certified (1000 < 1001), and the count pass runs again before the final pass.
DROP TABLE IF EXISTS t_two_pass_recount;
CREATE TABLE t_two_pass_recount (k UInt32, s String) ENGINE = MergeTree PARTITION BY intDiv(k, 1000) ORDER BY tuple()
SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_two_pass_recount SELECT number % 400 AS k, toString(k) AS s FROM numbers(400000);

SELECT 'results';
-- OFFSET, and ties at the boundary broken by the other sort columns.
SELECT k, s, count() FROM t_two_pass_uniform GROUP BY k, s ORDER BY count() DESC, k, s LIMIT 10 OFFSET 95 SETTINGS log_comment = 'two_pass_q2_on';
-- Not certified: the final pass.
SELECT k, s, count() FROM t_two_pass_flat GROUP BY k, s ORDER BY count() DESC, k, s LIMIT 10 SETTINGS log_comment = 'two_pass_q5_on';
-- The final pass with an OFFSET.
SELECT k, s, count() FROM t_two_pass_flat GROUP BY k, s ORDER BY count() DESC, k, s LIMIT 5 OFFSET 5 SETTINGS log_comment = 'two_pass_q8_on';
-- The count pass again, then the final pass.
SELECT k, s, count() FROM t_two_pass_recount GROUP BY k, s ORDER BY count() DESC, k, s LIMIT 10 SETTINGS log_comment = 'two_pass_q9_on';
-- Only the counts are determined when the boundary has ties and there is no other sort column.
SELECT arraySort(groupArray(c)) FROM (SELECT k, s, count() AS c FROM t_two_pass_uniform GROUP BY k, s ORDER BY c DESC LIMIT 50) SETTINGS log_comment = 'two_pass_q6_on';

SYSTEM FLUSH LOGS query_log;

SELECT 'passes, fallbacks, all rows of every pass in read_rows';
SELECT log_comment, ProfileEvents['TwoPassCountTopKPasses'], ProfileEvents['TwoPassCountTopKFallbacks'], read_rows > 1100000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'two\_pass\_%'
ORDER BY log_comment;

DROP TABLE t_two_pass_flat;
DROP TABLE t_two_pass_uniform;
DROP TABLE t_two_pass_recount;
