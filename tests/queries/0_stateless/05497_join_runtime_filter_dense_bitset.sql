SET enable_parallel_replicas = 0, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, join_runtime_filter_exact_values_limit = 10000;
SET join_runtime_filter_from_fixed_hash_table = 0;
SET join_algorithm = 'hash', query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS t_probe;
DROP TABLE IF EXISTS t_build;
CREATE TABLE t_probe (k Int16) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_build (k Int16) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_probe SELECT toInt16(number - 32768) FROM numbers(65536);
INSERT INTO t_build SELECT toInt16(number * 3 - 32768) FROM numbers(20000);

SELECT count() FROM t_probe AS p INNER JOIN t_build AS b ON p.k = b.k SETTINGS log_comment = '05497_int16';
SELECT count() FROM t_probe AS p INNER JOIN (SELECT toInt32(k) AS k FROM t_build) AS b ON toInt32(p.k) = b.k SETTINGS log_comment = '05497_int32';
SELECT count() FROM t_probe AS p INNER JOIN (SELECT toUInt8(number * 3) AS k FROM numbers(80)) AS b ON toUInt8(p.k) = b.k SETTINGS join_runtime_filter_exact_values_limit = 50, log_comment = '05497_uint8';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['RuntimeFilterDenseBitsetsBuilt'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05497_int16', '05497_int32', '05497_uint8')
ORDER BY log_comment;

DROP TABLE t_probe;
DROP TABLE t_build;
