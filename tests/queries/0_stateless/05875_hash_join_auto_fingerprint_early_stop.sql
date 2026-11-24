SET enable_analyzer = 1, enable_parallel_replicas = 0, serialize_query_plan = 0;
SET join_algorithm = 'hash', hash_join_layout = 'serial', experimental_join_fingerprint_mode = 4;
SET query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 1, query_plan_optimize_join_order_randomize = 0;
SET query_plan_convert_join_to_in = 0, enable_join_runtime_filters = 0, enable_join_fixed_hash_table_conversion = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET max_threads = 1, max_block_size = 65536, preferred_block_size_bytes = 0;
SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_insert_block_size = 65536;
SET use_query_cache = 0, enable_positional_arguments = 0;
SET log_queries = 1, log_queries_probability = 1, log_profile_events = 1;
SET log_queries_min_query_duration_ms = 0, log_queries_min_type = 'QUERY_FINISH';

DROP VIEW IF EXISTS fp_stop_join;
DROP TABLE IF EXISTS fp_stop_probe;
DROP TABLE IF EXISTS fp_stop_build;
CREATE TABLE fp_stop_probe (id UInt64, k UInt64) ENGINE = Memory;
CREATE TABLE fp_stop_build (k UInt64, v UInt64) ENGINE = Memory;
INSERT INTO fp_stop_build SELECT number * 2, number FROM numbers(65536);
-- An ordered first block of distinct misses qualifies for automatic preparation. Only two
-- probes in the next block match, so every early-stopping query returns a real row.
INSERT INTO fp_stop_probe
SELECT number, if(number IN (65536, 65537), 2 * (number - 65536), number * 2 + 1) FROM numbers(1048576);
CREATE VIEW fp_stop_join AS
SELECT p.id, b.v FROM (SELECT * FROM fp_stop_probe ORDER BY id) AS p ALL INNER JOIN fp_stop_build AS b USING (k);

SELECT id FROM fp_stop_join LIMIT 1 AFTER id >= 0 SETTINGS log_comment = '05875_range';
SELECT DISTINCT toUInt8(7) AS x FROM fp_stop_join SETTINGS log_comment = '05875_const_distinct';
SELECT toUInt8(7) AS x FROM fp_stop_join LIMIT 1 BY x SETTINGS log_comment = '05875_const_limit_by';
-- With nonconstant keys both matching rows must be consumed; these shapes remain eligible.
SELECT DISTINCT v FROM fp_stop_join ORDER BY v SETTINGS log_comment = '05875_full_distinct';
SELECT v FROM fp_stop_join ORDER BY v LIMIT 1 BY v SETTINGS log_comment = '05875_full_limit_by';

-- Result limits run after the plan. The outer cap also applies when automatic mode is enabled only inside a subquery.
SELECT id, v FROM fp_stop_join SETTINGS max_result_rows = 1, result_overflow_mode = 'break', log_comment = '05875_result_rows' FORMAT Null;
SELECT id, v FROM (SELECT id, v FROM fp_stop_join SETTINGS experimental_join_fingerprint_mode = 4)
SETTINGS experimental_join_fingerprint_mode = 0, max_result_bytes = 1, result_overflow_mode = 'break', log_comment = '05875_result_bytes' FORMAT Null;
SELECT v, count() FROM fp_stop_join GROUP BY v
SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'break', log_comment = '05875_group_break' FORMAT Null;
-- An inner aggregation retains its own overflow mode.
SELECT * FROM (SELECT v, count() FROM fp_stop_join GROUP BY v SETTINGS max_rows_to_group_by = 1, group_by_overflow_mode = 'break')
SETTINGS group_by_overflow_mode = 'throw', log_comment = '05875_nested_group' FORMAT Null;
-- Nonthrowing limits on sorting and storage reads can also truncate a stream.
SELECT v FROM fp_stop_join ORDER BY v SETTINGS max_rows_to_sort = 131072, sort_overflow_mode = 'break', log_comment = '05875_sort_break' FORMAT Null;
SELECT id FROM fp_stop_join SETTINGS max_rows_to_read = 900000, read_overflow_mode = 'break', log_comment = '05875_read_break' FORMAT Null;
-- A throwing cap and an unbounded BREAK cannot truncate a successful query. Leaf-only caps do not apply on the initiator.
SELECT v, count() FROM fp_stop_join GROUP BY v SETTINGS max_rows_to_group_by = 2, group_by_overflow_mode = 'throw',
    max_result_rows = 2, result_overflow_mode = 'throw', log_comment = '05875_full_throw' FORMAT Null;
SELECT id FROM fp_stop_join SETTINGS max_result_rows = 0, max_result_bytes = 0, result_overflow_mode = 'break',
    max_rows_to_read_leaf = 1, read_overflow_mode_leaf = 'break', log_comment = '05875_full_uncapped' FORMAT Null;

SYSTEM FLUSH LOGS query_log;
WITH (SELECT lower(value) IN ('aarch64', 'arm64') FROM system.build_options WHERE name = 'SYSTEM_PROCESSOR') AS is_arm
SELECT log_comment,
       ProfileEvents['HashJoinAutoFingerprintCandidates'] > 0,
       (ProfileEvents['HashJoinAutoFingerprintSelected'] > 0) = (is_arm AND startsWith(log_comment, '05875_full_')),
       (ProfileEvents['HashJoinAutoFingerprintProbeEstimate'] > 0) = (is_arm AND startsWith(log_comment, '05875_full_'))
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND startsWith(log_comment, '05875_')
ORDER BY log_comment, event_time_microseconds DESC LIMIT 1 BY log_comment;

DROP VIEW fp_stop_join;
DROP TABLE fp_stop_probe;
DROP TABLE fp_stop_build;
