-- Two sessions whose changed string settings concatenate to the same text must not share query condition cache entries.
SET use_query_condition_cache = 1, optimize_move_to_prewhere = 0;

DROP TABLE IF EXISTS t_qcc_string_boundaries;
-- Without statistics, so that the verdict is written by the filter rather than the part being pruned first.
CREATE TABLE t_qcc_string_boundaries (id UInt64, b Bool, v Int64) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';
INSERT INTO t_qcc_string_boundaries SELECT number, true, 42 FROM numbers(20000);

-- Collides in both the settings salt and the format settings hash of `toString` / `equals`.
SELECT count() FROM t_qcc_string_boundaries WHERE toString(b) = 'x' SETTINGS bool_true_representation = 'xbool_false_representation', bool_false_representation = 'y';
SELECT count() FROM t_qcc_string_boundaries WHERE toString(b) = 'x' SETTINGS bool_true_representation = 'x', bool_false_representation = 'bool_false_representationy';
SELECT count() FROM t_qcc_string_boundaries WHERE toString(b) = 'x' SETTINGS use_query_condition_cache = 0, bool_true_representation = 'x', bool_false_representation = 'bool_false_representationy';

-- Collides only in the settings salt: `formatRowNoNewline` and `match` do not hash the format settings.
SELECT count() FROM t_qcc_string_boundaries WHERE NOT match(formatRowNoNewline('TSV', b), 'y') SETTINGS bool_true_representation = 'xy', bool_false_representation = 'z';
SELECT count() FROM t_qcc_string_boundaries WHERE NOT match(formatRowNoNewline('TSV', b), 'y') SETTINGS bool_true_representation = 'x', bool_false_representation = 'yz';
SELECT count() FROM t_qcc_string_boundaries WHERE NOT match(formatRowNoNewline('TSV', b), 'y') SETTINGS use_query_condition_cache = 0, bool_true_representation = 'x', bool_false_representation = 'yz';

-- Collides only in the format settings hash of `toJSONString` / `equals`: neither setting is in the salt.
SELECT count() FROM t_qcc_string_boundaries WHERE toJSONString(v) = '42' SETTINGS column_names_for_schema_inference = 'a', output_format_json_quote_64bit_integers = 1;
SELECT count() FROM t_qcc_string_boundaries WHERE toJSONString(v) = '42' SETTINGS column_names_for_schema_inference = 'aoutput_format_json_quote_64bit_integers1', output_format_json_quote_64bit_integers = 0;
SELECT count() FROM t_qcc_string_boundaries WHERE toJSONString(v) = '42' SETTINGS use_query_condition_cache = 0, column_names_for_schema_inference = 'aoutput_format_json_quote_64bit_integers1', output_format_json_quote_64bit_integers = 0;

DROP TABLE t_qcc_string_boundaries;
