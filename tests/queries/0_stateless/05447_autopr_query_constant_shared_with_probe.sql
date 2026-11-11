-- The plan with parallel replicas is built from a second analysis of the query and, when it is used, it takes over
-- the index analysis of the single-node plan. Both analyses must therefore evaluate a constant that is
-- non-deterministic but fixed within a query (`randConstant`, `now64`) to the same value: otherwise the marks are
-- selected for one cutoff and the rows are filtered by another, and rows go missing.
--
-- The cutoff varies in a narrow band so that statistics collected by the first query stay valid for the others,
-- which then adopt the plan with parallel replicas. `k` counts from zero, so `count()` equals the cutoff.

DROP TABLE IF EXISTS t_autopr_query_constant;

CREATE TABLE t_autopr_query_constant (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO t_autopr_query_constant SELECT number FROM numbers(10000000) SETTINGS max_rows_to_read = 0;

SET enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    automatic_parallel_replicas_min_bytes_per_replica = 0, serialize_query_plan = 0, max_threads = 1,
    max_rows_to_read = 0;
-- The cost model caps the reading threads at one per `merge_tree_min_bytes_per_task_for_remote_reading` read.
SET merge_tree_min_bytes_per_task_for_remote_reading = 1024;

SELECT count() = any(5000000 + randConstant() % 100000) FROM t_autopr_query_constant
WHERE k < 5000000 + randConstant() % 100000 SETTINGS log_comment = 'query_constant_1';
SELECT count() = any(5000000 + randConstant() % 100000) FROM t_autopr_query_constant
WHERE k < 5000000 + randConstant() % 100000 SETTINGS log_comment = 'query_constant_2';
SELECT count() = any(5000000 + randConstant() % 100000) FROM t_autopr_query_constant
WHERE k < 5000000 + randConstant() % 100000 SETTINGS log_comment = 'query_constant_3';
SELECT count() = any(5000000 + randConstant() % 100000) FROM t_autopr_query_constant
WHERE k < 5000000 + randConstant() % 100000 SETTINGS log_comment = 'query_constant_4';
SELECT count() = any(5000000 + randConstant() % 100000) FROM t_autopr_query_constant
WHERE k < 5000000 + randConstant() % 100000 SETTINGS log_comment = 'query_constant_5';
SELECT count() = any(5000000 + randConstant() % 100000) FROM t_autopr_query_constant
WHERE k < 5000000 + randConstant() % 100000 SETTINGS log_comment = 'query_constant_6';
SELECT count() = any(5000000 + randConstant() % 100000) FROM t_autopr_query_constant
WHERE k < 5000000 + randConstant() % 100000 SETTINGS log_comment = 'query_constant_7';
SELECT count() = any(5000000 + randConstant() % 100000) FROM t_autopr_query_constant
WHERE k < 5000000 + randConstant() % 100000 SETTINGS log_comment = 'query_constant_8';

-- `getSetting` is not shared between the analyses: the probe turns `automatic_parallel_replicas_mode` off, so it
-- reads another value than the single-node plan, and plans with different cutoffs must not be matched.
SELECT count() = any(5000000 - 100000 * getSetting('automatic_parallel_replicas_mode')) FROM t_autopr_query_constant
WHERE k < 5000000 - 100000 * getSetting('automatic_parallel_replicas_mode') SETTINGS log_comment = 'setting_1';
SELECT count() = any(5000000 - 100000 * getSetting('automatic_parallel_replicas_mode')) FROM t_autopr_query_constant
WHERE k < 5000000 - 100000 * getSetting('automatic_parallel_replicas_mode') SETTINGS log_comment = 'setting_2';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- The first query collects statistics, the others use the plan with parallel replicas.
SELECT log_comment, ProfileEvents['AutoParallelReplicasApplied'] AS applied
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment LIKE 'query_constant_%'
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP TABLE t_autopr_query_constant;
