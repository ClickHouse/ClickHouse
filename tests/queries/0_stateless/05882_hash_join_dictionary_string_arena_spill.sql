-- New dictionary keys are copied into the hash table's arena during serial replay.
SET join_algorithm = 'hash', hash_join_layout = 'serial', max_threads = 1, max_block_size = 8192,
    max_bytes_ratio_before_external_join = 0, enable_packed_string_keys_in_hash_join = 0,
    collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = false,
    query_plan_optimize_join_order_randomize = 0, max_bytes_in_join = 0, max_rows_in_join = 0;

CREATE VIEW dictionary_keys AS
SELECT toLowCardinality(leftPad(toString(number), 2048, '0')) AS k, number AS v FROM numbers(4096);
CREATE VIEW fixed_dictionary_keys AS
SELECT toLowCardinality(toFixedString(toString(number), 2048)) AS k, number AS v FROM numbers(4096);

SELECT count(), sum(r.v) FROM dictionary_keys AS l INNER ALL JOIN dictionary_keys AS r USING (k)
SETTINGS max_bytes_before_external_join = 24000000, log_comment = '05882_string_tight';
SELECT count(), sum(r.v) FROM dictionary_keys AS l INNER ALL JOIN dictionary_keys AS r USING (k)
SETTINGS max_bytes_before_external_join = 120000000, log_comment = '05882_string_wide';
SELECT count(), sum(r.v) FROM fixed_dictionary_keys AS l INNER ALL JOIN fixed_dictionary_keys AS r USING (k)
SETTINGS max_bytes_before_external_join = 24000000, log_comment = '05882_fixed_tight';
SELECT count(), sum(r.v) FROM fixed_dictionary_keys AS l INNER ALL JOIN fixed_dictionary_keys AS r USING (k)
SETTINGS max_bytes_before_external_join = 120000000, log_comment = '05882_fixed_wide';

-- A filtered block retains the dictionary, but all its surviving rows reference one key.
CREATE TABLE retained_dictionary (k LowCardinality(String), v UInt64) ENGINE = Memory;
INSERT INTO retained_dictionary
SELECT leftPad(toString(if(number < 4096, 0, number)), 2048, '0'), number FROM numbers(8192);

SELECT count(), sum(r.v)
FROM (SELECT k FROM retained_dictionary WHERE v = 0) AS l
INNER ALL JOIN (SELECT k, v FROM retained_dictionary WHERE v < 4096) AS r USING (k)
SETTINGS max_bytes_before_external_join = 24000000, log_comment = '05882_filtered';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05882_string_tight', '05882_string_wide', '05882_fixed_tight', '05882_fixed_wide', '05882_filtered')
ORDER BY log_comment;

DROP TABLE retained_dictionary;
DROP VIEW fixed_dictionary_keys;
DROP VIEW dictionary_keys;
