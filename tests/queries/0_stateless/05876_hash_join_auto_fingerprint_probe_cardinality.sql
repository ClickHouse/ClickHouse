SET enable_analyzer = 1, enable_parallel_replicas = 0, serialize_query_plan = 0;
SET join_algorithm = 'hash', hash_join_layout = 'serial', experimental_join_fingerprint_mode = 4;
SET query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 1, query_plan_optimize_join_order_randomize = 0;
SET query_plan_convert_join_to_in = 0, enable_join_runtime_filters = 0, enable_join_fixed_hash_table_conversion = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET max_threads = 1, max_block_size = 65536, preferred_block_size_bytes = 0;
SET group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;
SET aggregate_functions_null_for_empty = 0, use_query_cache = 0;
SET log_queries = 1, log_queries_probability = 1, log_profile_events = 1;
SET log_queries_min_query_duration_ms = 0, log_queries_min_type = 'QUERY_FINISH';

DROP TABLE IF EXISTS fp_card_build;
DROP TABLE IF EXISTS fp_card_arrays;
DROP TABLE IF EXISTS fp_card_probe;
CREATE TABLE fp_card_build (k UInt64, v UInt64) ENGINE = Memory;
CREATE TABLE fp_card_arrays (keys Array(UInt64)) ENGINE = Memory;
CREATE TABLE fp_card_probe (k UInt64) ENGINE = Memory;
INSERT INTO fp_card_build SELECT number, number + 1 FROM numbers(131072);
INSERT INTO fp_card_arrays SELECT [number + 1048576] FROM numbers(32768);
INSERT INTO fp_card_probe SELECT if(number = 4194303, 5, number + 1048576) FROM numbers(4194304);

-- Every array has one element. The join-order estimate assumes an expansion factor of 100.
SELECT count(), sum(b.v)
FROM (SELECT arrayJoin(keys) AS k FROM fp_card_arrays) AS p ALL INNER JOIN fp_card_build AS b USING (k)
SETTINGS log_comment = '05876_array';

-- Without distinct-count statistics the grouping estimate keeps the input row count.
SELECT count(), sum(b.v)
FROM (SELECT 1048576 + k % 4096 AS grouped_key FROM fp_card_probe GROUP BY grouped_key) AS p
ALL INNER JOIN fp_card_build AS b ON p.grouped_key = b.k
SETTINGS log_comment = '05876_group';

-- The same input without grouping supplies a real long probe stream. The final key matches.
SELECT count(), sum(b.v)
FROM fp_card_probe AS p ALL INNER JOIN fp_card_build AS b USING (k)
SETTINGS log_comment = '05876_plain';

-- A join-key expression can change the probe count even when the child is a plain read.
TRUNCATE TABLE fp_card_arrays;
INSERT INTO fp_card_arrays SELECT if(number < 4096, [number + 1048576], []) FROM numbers(4194304);
SELECT count(), sum(b.v)
FROM fp_card_arrays AS p ALL INNER JOIN fp_card_build AS b ON arrayJoin(p.keys) = b.k
SETTINGS query_plan_optimize_join_order_limit = 0, log_comment = '05876_direct';

SYSTEM FLUSH LOGS query_log;
WITH (SELECT lower(value) IN ('aarch64', 'arm64') FROM system.build_options WHERE name = 'SYSTEM_PROCESSOR') AS is_arm
SELECT log_comment,
       ProfileEvents['JoinProbeTableRowCount'] AS actual_probes,
       ProfileEvents['HashJoinAutoFingerprintCandidates'] = 1,
       (ProfileEvents['HashJoinAutoFingerprintProbeEstimate'] > 0) = (is_arm AND log_comment = '05876_plain'),
       (ProfileEvents['HashJoinAutoFingerprintSelected'] > 0) = (is_arm AND log_comment = '05876_plain')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05876_array', '05876_direct', '05876_group', '05876_plain')
ORDER BY log_comment, event_time_microseconds DESC LIMIT 1 BY log_comment;

DROP TABLE fp_card_probe;
DROP TABLE fp_card_arrays;
DROP TABLE fp_card_build;
