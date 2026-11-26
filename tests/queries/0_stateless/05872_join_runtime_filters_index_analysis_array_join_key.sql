-- a JOIN runtime filter on an element of an array column skips granules with the index of the array

SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
-- the counters depend on the side that builds the filter
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
-- with parallel replicas the counters depend on the assignment of the granules
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_rf_aj;
DROP TABLE IF EXISTS t_rf_aj_lookup;

CREATE TABLE t_rf_aj
(
    id UInt64,
    ids Array(String),
    txt Array(String),
    fs Array(FixedString(4)),
    nums Array(UInt32),
    INDEX idx_ids ids TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_txt txt TYPE text(tokenizer = 'array'),
    INDEX idx_fs fs TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_nums nums TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;

CREATE TABLE t_rf_aj_lookup (v String, fv FixedString(4), n UInt32) ENGINE = MergeTree ORDER BY v;

-- rows 1000 to 1015 are one granule of empty arrays
INSERT INTO t_rf_aj SELECT
    number,
    if(number BETWEEN 1000 AND 1015, [], [toString(number), toString(number + 100000)]),
    if(number BETWEEN 1000 AND 1015, [], [toString(number), toString(number + 100000)]),
    if(number BETWEEN 1000 AND 1015, [], [toFixedString(leftPad(toString(number), 4, '0'), 4)]),
    [number, number + 100000]
FROM numbers(2000);

INSERT INTO t_rf_aj_lookup SELECT toString(number), toFixedString(leftPad(toString(number), 4, '0'), 4), number FROM numbers(5);

SELECT '-- the results are the same as without the runtime filter';
SELECT count(), sum(id) FROM t_rf_aj ARRAY JOIN ids AS x JOIN t_rf_aj_lookup AS l ON x = l.v SETTINGS enable_join_runtime_filters = 0;

SELECT count(), sum(id) FROM t_rf_aj ARRAY JOIN ids AS x JOIN t_rf_aj_lookup AS l ON x = l.v SETTINGS log_comment = 'rf_aj_01_bloom_array_join';
SELECT count(), sum(id) FROM t_rf_aj JOIN t_rf_aj_lookup AS l ON arrayJoin(ids) = l.v SETTINGS log_comment = 'rf_aj_02_bloom_function';
SELECT count(), sum(id) FROM t_rf_aj LEFT ARRAY JOIN ids AS x JOIN t_rf_aj_lookup AS l ON x = l.v SETTINGS log_comment = 'rf_aj_03_bloom_left';
SELECT count(), sum(id) FROM t_rf_aj ARRAY JOIN txt AS x JOIN t_rf_aj_lookup AS l ON x = l.v SETTINGS log_comment = 'rf_aj_04_text_array_join';
SELECT count(), sum(id) FROM t_rf_aj JOIN t_rf_aj_lookup AS l ON arrayJoin(txt) = l.v SETTINGS log_comment = 'rf_aj_05_text_function';
SELECT count(), sum(id) FROM t_rf_aj ARRAY JOIN fs AS x JOIN t_rf_aj_lookup AS l ON x = l.fv SETTINGS log_comment = 'rf_aj_06_fixed_string';
SELECT count(), sum(id) FROM t_rf_aj ARRAY JOIN ids AS x, fs AS y JOIN t_rf_aj_lookup AS l ON x = l.v WHERE x != '1' SETTINGS log_comment = 'rf_aj_07_two_arrays_and_filter';

SELECT '-- the default element of LEFT ARRAY JOIN is in the filter: the granule of empty arrays stays';
SELECT count(), sum(id) FROM t_rf_aj LEFT ARRAY JOIN ids AS x JOIN (SELECT v FROM t_rf_aj_lookup UNION ALL SELECT '') AS l ON x = l.v SETTINGS enable_join_runtime_filters = 0;
SELECT count(), sum(id) FROM t_rf_aj LEFT ARRAY JOIN ids AS x JOIN (SELECT v FROM t_rf_aj_lookup UNION ALL SELECT '') AS l ON x = l.v SETTINGS log_comment = 'rf_aj_08_bloom_left_default';
SELECT count(), sum(id) FROM t_rf_aj LEFT ARRAY JOIN txt AS x JOIN (SELECT v FROM t_rf_aj_lookup UNION ALL SELECT '') AS l ON x = l.v SETTINGS log_comment = 'rf_aj_09_text_left_default';
SELECT count(), sum(id) FROM t_rf_aj LEFT ARRAY JOIN fs AS x JOIN (SELECT fv FROM t_rf_aj_lookup UNION ALL SELECT toFixedString('', 4)) AS l ON x = l.fv SETTINGS log_comment = 'rf_aj_10_fixed_string_left_default';

SELECT '-- not used: a set index, LEFT ANTI JOIN, the setting is off, too many values';
SELECT count(), sum(id) FROM t_rf_aj ARRAY JOIN nums AS x JOIN t_rf_aj_lookup AS l ON x = l.n SETTINGS log_comment = 'rf_aj_11_set_index';
SELECT count(), sum(id) FROM t_rf_aj ARRAY JOIN ids AS x LEFT ANTI JOIN t_rf_aj_lookup AS l ON x = l.v SETTINGS log_comment = 'rf_aj_12_anti';
SELECT count(), sum(id) FROM t_rf_aj ARRAY JOIN ids AS x JOIN t_rf_aj_lookup AS l ON x = l.v SETTINGS log_comment = 'rf_aj_13_setting_off', enable_join_runtime_filters_index_analysis = 0;
-- the limit of 100 values lets these indexes take one value, and the filter has five
SELECT count(), sum(id) FROM t_rf_aj ARRAY JOIN ids AS x JOIN t_rf_aj_lookup AS l ON x = l.v SETTINGS log_comment = 'rf_aj_14_bloom_many_values', join_runtime_filter_exact_values_limit = 100;
SELECT count(), sum(id) FROM t_rf_aj ARRAY JOIN txt AS x JOIN t_rf_aj_lookup AS l ON x = l.v SETTINGS log_comment = 'rf_aj_15_text_many_values', join_runtime_filter_exact_values_limit = 100;

SELECT '-- a filter or a second key below ARRAY JOIN keeps the index of the array in use';
SELECT count(), sum(id) FROM t_rf_aj ARRAY JOIN ids AS x JOIN t_rf_aj_lookup AS l ON x = l.v WHERE id != 1999 SETTINGS log_comment = 'rf_aj_16_probe_filter';
SELECT count(), sum(id) FROM t_rf_aj ARRAY JOIN ids AS x JOIN t_rf_aj_lookup AS l ON x = l.v AND id % 10 = l.n SETTINGS log_comment = 'rf_aj_17_second_key';

SELECT '-- a key column with the name of the element expression is not taken for the element';
DROP TABLE IF EXISTS t_rf_aj_named;
CREATE TABLE t_rf_aj_named (ids Array(String), `arrayJoin(ids)` String, k UInt32, INDEX idx_ids ids TYPE bloom_filter(0.001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY (`arrayJoin(ids)`, k) SETTINGS index_granularity = 1;
INSERT INTO t_rf_aj_named SELECT [toString(number)], 'other' || toString(number), number FROM numbers(200);
SELECT count(), sum(k) FROM t_rf_aj_named ARRAY JOIN ids AS x JOIN t_rf_aj_lookup AS l ON x = l.v AND k = l.n SETTINGS enable_join_runtime_filters = 0;
SELECT count(), sum(k) FROM t_rf_aj_named ARRAY JOIN ids AS x JOIN t_rf_aj_lookup AS l ON x = l.v AND k = l.n SETTINGS log_comment = 'rf_aj_18_named_key_column';
DROP TABLE t_rf_aj_named;

SYSTEM FLUSH LOGS query_log;

SELECT '-- granules skipped by the runtime filter';
SELECT log_comment, argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) > 100 AS most, argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) > 0 AS some
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'rf\_aj\_%'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_rf_aj;
DROP TABLE t_rf_aj_lookup;
