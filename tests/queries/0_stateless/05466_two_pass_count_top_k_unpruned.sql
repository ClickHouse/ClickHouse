-- Tags: no-parallel-replicas
-- no-parallel-replicas: the optimization does not apply with parallel replicas.

SET query_plan_two_pass_count_top_k = 1, query_plan_two_pass_count_top_k_min_rows = 0;
SET enable_parallel_replicas = 0, use_query_condition_cache = 0, optimize_aggregation_in_order = 0, serialize_query_plan = 0;
-- Read, sort and speed limits disable the optimization: the test profile sets them all.
SET max_rows_to_read = 0, max_bytes_to_read = 0, max_bytes_to_read_leaf = 0, max_rows_to_group_by = 0, max_rows_to_sort = 0,
    max_bytes_to_sort = 0, max_execution_speed = 0, max_execution_speed_bytes = 0, max_estimated_execution_time = 0;

-- A key outside the primary key: every pass reads all granules, so the restricted pass covers the most frequent keys holding
-- up to a tenth of the rows. The 10 largest groups (1000 rows) belong to keys ranked below 150 keys of 1500 rows over many
-- strings; the restricted pass over 460 keys is the result, without another pass.
DROP TABLE IF EXISTS t_two_pass_unpruned;
CREATE TABLE t_two_pass_unpruned (k UInt32, s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_two_pass_unpruned SELECT
    multiIf(r < 225000, intDiv(r, 1500), r < 237000, 1000 + intDiv(r - 225000, 1200), r < 417000, 2000 + intDiv(r - 237000, 600), 10000 + intDiv(r - 417000, 300)) AS k,
    if(r >= 225000 AND r < 237000 AND (r - 225000) % 1200 < 1000, 'top', toString(intHash32(number) % 1000)) AS s
FROM (SELECT number, (number * 1000003) % 2817000 AS r FROM numbers(2817000));

SELECT k, s, count() FROM t_two_pass_unpruned GROUP BY k, s ORDER BY count() DESC, k, s LIMIT 10 SETTINGS log_comment = 'two_pass_unpruned_on';

SYSTEM FLUSH LOGS query_log;

SELECT 'passes, fallbacks, all rows of every pass in read_rows';
SELECT log_comment, ProfileEvents['TwoPassCountTopKPasses'], ProfileEvents['TwoPassCountTopKFallbacks'], read_rows > 1100000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'two_pass_unpruned_on';

DROP TABLE t_two_pass_unpruned;
