SET enable_analyzer = 1, enable_parallel_replicas = 0, serialize_query_plan = 0;
SET join_algorithm = 'hash', hash_join_layout = 'serial', experimental_join_fingerprint_mode = 4;
SET query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 0, query_plan_convert_join_to_in = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 0, enable_join_fixed_hash_table_conversion = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET max_threads = 2, max_threads_min_free_memory_per_thread = 0, max_streams_to_max_threads_ratio = 1;
SET optimize_read_in_order = 1, max_block_size = 65536, explain_query_plan_default = 'legacy';
SET join_use_nulls = 0, aggregate_functions_null_for_empty = 0;
SET log_queries_min_query_duration_ms = 0, log_queries_min_type = 'QUERY_FINISH';
SET use_query_cache = 0, log_queries = 1, log_queries_probability = 1, log_profile_events = 1;

DROP TABLE IF EXISTS fp_range_probe;
DROP TABLE IF EXISTS fp_range_build;
CREATE TABLE fp_range_probe (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024;
CREATE TABLE fp_range_build (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024;

-- Interleaved, disjoint keys give both reads the same primary-key extent.
-- Each range has approximately ten probes per build key, below the selector's minimum of sixteen.
INSERT INTO fp_range_probe SELECT number * 2 + 1 FROM numbers(1600000);
INSERT INTO fp_range_build SELECT number * 20, number FROM numbers(160000);

SET query_plan_join_shard_by_pk_ranges = 0;
SELECT count(), sum(b.v) FROM fp_range_probe AS p ALL INNER JOIN fp_range_build AS b USING (k)
SETTINGS log_comment = '05874_unsharded';

SET query_plan_join_shard_by_pk_ranges = 1;
SELECT countIf(explain LIKE '%Sharding:%') > 0 FROM
    (EXPLAIN actions = 1 SELECT count(), sum(b.v) FROM fp_range_probe AS p ALL INNER JOIN fp_range_build AS b USING (k));
SELECT count(), sum(b.v) FROM fp_range_probe AS p ALL INNER JOIN fp_range_build AS b USING (k)
SETTINGS log_comment = '05874_sharded';

SYSTEM FLUSH LOGS query_log;

-- The ordinary join has a probe estimate on Arm. Every executed range clone must reject
-- the global estimate, even when it would pass the work budget against the smaller build.
WITH (SELECT lower(value) IN ('aarch64', 'arm64') FROM system.build_options WHERE name = 'SYSTEM_PROCESSOR') AS is_arm
SELECT
    ProfileEvents['HashJoinAutoFingerprintRejected'] = 1,
    (ProfileEvents['HashJoinAutoFingerprintProbeEstimate'] > 0) = is_arm
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05874_unsharded'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT
    ProfileEvents['HashJoinAutoFingerprintRejected'] > 1,
    ProfileEvents['HashJoinAutoFingerprintProbeEstimate'] = 0,
    ProfileEvents['HashJoinAutoFingerprintSelected'] = 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05874_sharded'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE fp_range_build;
DROP TABLE fp_range_probe;
