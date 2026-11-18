-- Tags: no-sanitizers
-- no-sanitizers: needs enough data and small blocks to collect statistics and make parallel replicas
--                cost-beneficial, which is too slow under sanitizers (as for 04341).

-- When automatic parallel replicas switches a query to the plan with parallel replicas, the reads of
-- that plan adopt the skip indexes of the single-node reads, and with `use_skip_indexes_on_data_read`
-- they are evaluated during the read. A `set` index over a column filtered by `IN (subquery)` then
-- evaluates the set of the single-node plan, which must still be there after it was transplanted.

DROP TABLE IF EXISTS t_autopr_skip_in;
DROP TABLE IF EXISTS t_autopr_skip_in_keys;

CREATE TABLE t_autopr_skip_in (org UInt64, d UInt64, payload String, INDEX idx_org org TYPE set(1000) GRANULARITY 1)
ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 128;
INSERT INTO t_autopr_skip_in SELECT (number % 1000) * 5, number, repeat('x', 20) FROM numbers(200000);

CREATE TABLE t_autopr_skip_in_keys (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_autopr_skip_in_keys SELECT number FROM numbers(0, 5000, 3);

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SELECT sum(cityHash64(payload, d)) FROM t_autopr_skip_in WHERE org IN (SELECT id FROM t_autopr_skip_in_keys);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_index_analysis_only_on_coordinator = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, use_query_condition_cache = 0;
-- Small blocks so enough are fed to the statistics collector.
SET max_threads = 4, max_block_size = 128;
SET automatic_parallel_replicas_min_bytes_per_replica = 0, merge_tree_min_bytes_per_task_for_remote_reading = 0;
-- External aggregation is not supported, i.e. no statistics would be reported.
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

-- Read a column rather than `count()`, which lets the plan with parallel replicas be skipped.
-- The first run collects statistics, the second one applies the plan with parallel replicas.
SELECT sum(cityHash64(payload, d)) FROM t_autopr_skip_in WHERE org IN (SELECT id FROM t_autopr_skip_in_keys) SETTINGS log_comment = '05829_collect';
SELECT sum(cityHash64(payload, d)) FROM t_autopr_skip_in WHERE org IN (SELECT id FROM t_autopr_skip_in_keys) SETTINGS log_comment = '05829_apply';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['AutoParallelReplicasApplied'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('05829_collect', '05829_apply') AND type = 'QueryFinish' AND is_initial_query
ORDER BY event_time_microseconds;

DROP TABLE t_autopr_skip_in;
DROP TABLE t_autopr_skip_in_keys;
