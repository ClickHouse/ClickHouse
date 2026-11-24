-- Each new key is larger than the space left in the previous arena chunk.
SET join_algorithm = 'hash', hash_join_layout = 'serial', max_threads = 1, max_block_size = 8192,
    max_bytes_ratio_before_external_join = 0, enable_packed_string_keys_in_hash_join = 0,
    collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0, max_bytes_in_join = 0, max_rows_in_join = 0;

CREATE VIEW variable_keys AS
SELECT repeat(repeat('x', 1400), bitShiftLeft(toUInt64(1), number)) AS k, number AS v FROM numbers(14);

SELECT count(), sum(r.v) FROM variable_keys AS l INNER ALL JOIN variable_keys AS r USING (k)
SETTINGS max_bytes_before_external_join = 90000000, log_comment = '05881_tight';
SELECT count(), sum(r.v) FROM variable_keys AS l INNER ALL JOIN variable_keys AS r USING (k)
SETTINGS max_bytes_before_external_join = 200000000, log_comment = '05881_wide';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05881_tight', '05881_wide')
ORDER BY log_comment;

DROP VIEW variable_keys;
