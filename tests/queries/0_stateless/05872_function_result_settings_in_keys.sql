-- A function such as `formatDateTime` or `locate` returns other values when the session changes one of its result settings.
-- The values stored with a table (sorting key, partition key, skip index) were computed with the settings of the table, so a
-- session that changes the setting must not use them.

SET formatdatetime_parsedatetime_m_is_month_name = 1, function_locate_has_mysql_compatible_argument_order = 1;

DROP TABLE IF EXISTS t_sort;
DROP TABLE IF EXISTS t_locate;
DROP TABLE IF EXISTS t_locate_index;
DROP TABLE IF EXISTS t_nested;
DROP TABLE IF EXISTS t_nested_sample;
DROP TABLE IF EXISTS t_lambda;
DROP TABLE IF EXISTS t_part;
DROP TABLE IF EXISTS t_format_row;
DROP TABLE IF EXISTS t_json_string;
DROP TABLE IF EXISTS t_json_to_string;
DROP TABLE IF EXISTS t_time_zone;
DROP TABLE IF EXISTS t_date_of_time;
DROP TABLE IF EXISTS t_explicit_time_zone;
DROP TABLE IF EXISTS t_if_numbers;
DROP TABLE IF EXISTS t_cast_number;
DROP TABLE IF EXISTS t_json_inference;
DROP TABLE IF EXISTS t_array_time_zone;
DROP TABLE IF EXISTS t_date_to_time;
DROP TABLE IF EXISTS t_dynamic_time;
DROP TABLE IF EXISTS t_json_has;
DROP TABLE IF EXISTS t_time_as_text;
DROP TABLE IF EXISTS t_time_as_text_index;
DROP TABLE IF EXISTS t_json_float;
DROP TABLE IF EXISTS t_json_overflow;
DROP TABLE IF EXISTS t_bool_cast;
DROP TABLE IF EXISTS t_json_inferred_time;
DROP TABLE IF EXISTS t_time_array_text;
DROP TABLE IF EXISTS t_unrelated_setting;
DROP TABLE IF EXISTS t_shared_prefix;
DROP TABLE IF EXISTS t_lambda_text;
DROP TABLE IF EXISTS t_plus_int;
DROP TABLE IF EXISTS t_start_of_day;
DROP TABLE IF EXISTS t_split_ranges;
DROP TABLE IF EXISTS t_json_parser;
DROP TABLE IF EXISTS t_json_raw_quote;
DROP TABLE IF EXISTS t_json_text_quote;
DROP TABLE IF EXISTS t_dynamic_time_text;
DROP TABLE IF EXISTS t_json_string_time;
DROP TABLE IF EXISTS t_format_row_time;

CREATE TABLE t_sort (dt DateTime('UTC'), b UInt32) ENGINE = MergeTree ORDER BY (formatDateTime(dt, '%M'), b) SETTINGS index_granularity = 100;
INSERT INTO t_sort SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + (number % 12) * 32 * 86400 + intDiv(number, 12) % 60 * 60, number FROM numbers(10000);
INSERT INTO t_sort SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + (number % 12) * 32 * 86400 + intDiv(number, 12) % 60 * 60, number FROM numbers(10000);

CREATE TABLE t_locate (s String) ENGINE = MergeTree ORDER BY locate(s, 'b') SETTINGS index_granularity = 1;
INSERT INTO t_locate VALUES ('abc'), ('b'), ('xxxxb'), ('zz');

CREATE TABLE t_locate_index (s String, INDEX idx locate(s, 'b') TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_locate_index SELECT concat(repeat('x', number), 'b') FROM numbers(3);

-- The `formatDateTime` call is folded into a constant together with `upper` when the key is built.
CREATE TABLE t_nested (s String) ENGINE = MergeTree ORDER BY concat(s, upper(formatDateTime(toDateTime('2024-05-01 00:05:00', 'UTC'), '%M'))) SETTINGS index_granularity = 1;
INSERT INTO t_nested SELECT toString(number) FROM numbers(1000);
CREATE TABLE t_nested_sample (s String) ENGINE = MergeTree ORDER BY cityHash64(s, upper(formatDateTime(toDateTime('2024-05-01 00:05:00', 'UTC'), '%M')))
    SAMPLE BY cityHash64(s, upper(formatDateTime(toDateTime('2024-05-01 00:05:00', 'UTC'), '%M'))) SETTINGS index_granularity = 1;
INSERT INTO t_nested_sample SELECT * FROM t_nested;

CREATE TABLE t_lambda (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY arrayMap(x -> formatDateTime(x, '%M'), [dt]) SETTINGS index_granularity = 1;
INSERT INTO t_lambda SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + number * 32 * 86400 + number * 60 FROM numbers(12);

CREATE TABLE t_part (dt DateTime('UTC'), b UInt32) ENGINE = MergeTree PARTITION BY formatDateTime(dt, '%M') ORDER BY b;
INSERT INTO t_part SELECT * FROM t_sort;

-- `formatRow` depends on every format setting, `toJSONString` on the settings of `JSON`, and parsing a time on the time zone.
CREATE TABLE t_format_row (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY formatRowNoNewline('CSV', x, y) SETTINGS index_granularity = 1;
INSERT INTO t_format_row SELECT 0, number FROM numbers(10000);
CREATE TABLE t_json_string (j JSON(a Nullable(Int64))) ENGINE = MergeTree ORDER BY toJSONString(j) SETTINGS index_granularity = 1;
INSERT INTO t_json_string SELECT '{"a":' || toString(number) || '}' FROM numbers(100);
INSERT INTO t_json_string VALUES ('{"a":null}');
CREATE TABLE t_json_to_string (j JSON(a Nullable(Int64))) ENGINE = MergeTree ORDER BY toString(j) SETTINGS index_granularity = 1;
INSERT INTO t_json_to_string SELECT * FROM t_json_string;
-- The time zone of a `DateTime64` column is part of its type, and an explicit time zone does not depend on the session.
CREATE TABLE t_date_of_time (ts DateTime64(3)) ENGINE = MergeTree ORDER BY toDate(ts) SETTINGS index_granularity = 1;
INSERT INTO t_date_of_time SELECT toDateTime64('2024-01-01 00:00:00', 3) + number * 86400 FROM numbers(10);
CREATE TABLE t_explicit_time_zone (s String) ENGINE = MergeTree ORDER BY toDateTime(s, 'UTC') SETTINGS index_granularity = 1;
INSERT INTO t_explicit_time_zone SELECT toString(toDateTime('2024-01-01 00:00:00', 'UTC') + number * 3600) FROM numbers(48);
-- These settings change only a result of text, or a `Dynamic` or `Variant` value.
CREATE TABLE t_if_numbers (x Int64) ENGINE = MergeTree ORDER BY if(x >= 0, -x, x) SETTINGS index_granularity = 1;
INSERT INTO t_if_numbers SELECT number - 10 FROM numbers(20);
CREATE TABLE t_cast_number (s String) ENGINE = MergeTree ORDER BY CAST(s AS UInt64) SETTINGS index_granularity = 1;
INSERT INTO t_cast_number SELECT toString(number) FROM numbers(20);
-- `JSON` infers the types of its paths from text, and an array of text is parsed element by element.
CREATE TABLE t_json_inference (s String) ENGINE = MergeTree ORDER BY dynamicType(getSubcolumn(CAST(s AS JSON), 'a'))
    SETTINGS index_granularity = 1;
INSERT INTO t_json_inference SELECT concat('{"a":"2024-01-', leftPad(toString(number + 1), 2, '0'), '"}') FROM numbers(20);
INSERT INTO t_json_inference VALUES ('{"a":"xx"}');
CREATE TABLE t_array_time_zone (a Array(String)) ENGINE = MergeTree ORDER BY CAST(a AS Array(DateTime)) SETTINGS index_granularity = 1;
INSERT INTO t_array_time_zone SELECT [toString(toDateTime('2024-01-01 00:00:00', 'UTC') + number * 3600)] FROM numbers(48);
-- A `Date` has no time zone of its own: as a `DateTime` it is midnight in the time zone of the result.
CREATE TABLE t_date_to_time (d Date) ENGINE = MergeTree ORDER BY toDateTime(d) SETTINGS index_granularity = 1;
INSERT INTO t_date_to_time SELECT toDate('2024-01-01') + number FROM numbers(10);
-- A `Dynamic` value holds text at runtime, and `JSONHas` reads a `JSON` value.
CREATE TABLE t_dynamic_time (d Dynamic) ENGINE = MergeTree ORDER BY toDateTime(d) SETTINGS index_granularity = 1;
INSERT INTO t_dynamic_time SELECT CAST(toString(toDateTime('2024-01-01 00:00:00', 'UTC') + number * 3600) AS Dynamic) FROM numbers(48);
CREATE TABLE t_json_has (j JSON(a Nullable(Int64))) ENGINE = MergeTree ORDER BY JSONHas(j, 'a') SETTINGS index_granularity = 1;
INSERT INTO t_json_has VALUES ('{"a":null}'), ('{"a":1}');
-- `toDateTime` of a number creates a `DateTime` in the time zone of the session, so its text depends on it.
CREATE TABLE t_time_as_text (n UInt32) ENGINE = MergeTree ORDER BY toString(toDateTime(n)) SETTINGS index_granularity = 1;
INSERT INTO t_time_as_text SELECT toUnixTimestamp(toDateTime('2024-01-01 00:00:00', 'UTC')) + number * 3600 FROM numbers(48);
CREATE TABLE t_time_as_text_index (n UInt32, INDEX idx toString(toDateTime(n)) TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY n
    SETTINGS index_granularity = 1;
INSERT INTO t_time_as_text_index SELECT * FROM t_time_as_text;
-- A number in quotes is parsed with `precise_float_parsing`: `1.6725` or `1.6724999999999999`.
CREATE TABLE t_json_float (j String) ENGINE = MergeTree ORDER BY JSONExtractFloat(j, 'x') SETTINGS index_granularity = 1;
INSERT INTO t_json_float SELECT '{"x":"1.6725"}' FROM numbers(3);
INSERT INTO t_json_float SELECT concat('{"x":"', toString(number), '.5"}') FROM numbers(50);
-- `JSONExtract` saturates a `DateTime` that overflows, or returns the epoch with `date_time_overflow_behavior = 'throw'`.
CREATE TABLE t_json_overflow (s String) ENGINE = MergeTree ORDER BY JSONExtract(s, 'DateTime') SETTINGS index_granularity = 1;
INSERT INTO t_json_overflow VALUES ('4294967296');
INSERT INTO t_json_overflow SELECT toString(1000000 + number) FROM numbers(50);
-- The spellings of `Bool` are used to parse it from text.
CREATE TABLE t_bool_cast (s String) ENGINE = MergeTree ORDER BY CAST(s AS Bool) SETTINGS index_granularity = 1;
INSERT INTO t_bool_cast SELECT if(number % 2, 'true', 'false') FROM numbers(50);
-- A `DateTime` inferred in `JSON`, and a `DateTime` without a time zone written as text, use the time zone of the session.
CREATE TABLE t_json_inferred_time (s String) ENGINE = MergeTree
    ORDER BY assumeNotNull(toUInt32(dynamicElement(getSubcolumn(CAST(s AS JSON), 'a'), 'DateTime'))) SETTINGS index_granularity = 1;
INSERT INTO t_json_inferred_time SELECT concat('{"a":"2024-01-01 ', leftPad(toString(number), 2, '0'), ':00:00"}') FROM numbers(24);
CREATE TABLE t_time_array_text (dt DateTime) ENGINE = MergeTree ORDER BY toString([dt]) SETTINGS index_granularity = 1;
INSERT INTO t_time_array_text SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + number * 3600 FROM numbers(24);
-- Only the subexpressions computed from `formatDateTime` are unusable: the key column `lower(s)` is not.
CREATE TABLE t_shared_prefix (s String) ENGINE = MergeTree
    ORDER BY (lower(s), concat(lower(s), formatDateTime(toDateTime('2024-05-01', 'UTC'), '%M'))) SETTINGS index_granularity = 1;
INSERT INTO t_shared_prefix SELECT toString(number) FROM numbers(100);
CREATE TABLE t_lambda_text
(
    m Map(String, String),
    fm Map(String, String) ALIAS mapFilter((k, v) -> k != '', m),
    INDEX idx mapKeys(fm) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_lambda_text VALUES ({'abc':'1'}), ({'def':'1'});
CREATE TABLE t_plus_int (x UInt64) ENGINE = MergeTree ORDER BY x + 1 SETTINGS index_granularity = 1;
INSERT INTO t_plus_int SELECT number FROM numbers(100);
CREATE TABLE t_start_of_day (d Date) ENGINE = MergeTree ORDER BY toStartOfDay(d) SETTINGS index_granularity = 1;
INSERT INTO t_start_of_day SELECT toDate('2024-01-01') + number FROM numbers(10);
CREATE TABLE t_split_ranges (dt DateTime) ENGINE = MergeTree ORDER BY toString([dt]) SETTINGS index_granularity = 1;
SYSTEM STOP MERGES t_split_ranges;
INSERT INTO t_split_ranges SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + number * 3600 FROM numbers(24) SETTINGS session_timezone = 'UTC';
INSERT INTO t_split_ranges SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + number * 3600 FROM numbers(24) SETTINGS session_timezone = 'UTC';
CREATE TABLE t_json_parser (j String) ENGINE = MergeTree ORDER BY JSONType(j, 'a') SETTINGS index_granularity = 1;
INSERT INTO t_json_parser SELECT if(number = 0, '{"a":18446744073709551616}', '{"a":"x"}') FROM numbers(20);
CREATE TABLE t_json_raw_quote (j JSON(a UInt64)) ENGINE = MergeTree ORDER BY JSONExtractRaw(j, 'a') SETTINGS index_granularity = 1;
INSERT INTO t_json_raw_quote SELECT concat('{"a":', toString(number), '}') FROM numbers(20);
CREATE TABLE t_json_text_quote (j JSON(a UInt64)) ENGINE = MergeTree ORDER BY toString(j) SETTINGS index_granularity = 1;
INSERT INTO t_json_text_quote SELECT concat('{"a":', toString(number), '}') FROM numbers(20);
CREATE TABLE t_dynamic_time_text (d Dynamic) ENGINE = MergeTree ORDER BY toString(d) SETTINGS index_granularity = 1;
INSERT INTO t_dynamic_time_text SELECT [toDateTime('2024-01-01 00:00:00', 'UTC') + number * 3600]::Array(DateTime) FROM numbers(24);
CREATE TABLE t_json_string_time (dt DateTime) ENGINE = MergeTree ORDER BY toJSONString(dt) SETTINGS index_granularity = 1;
INSERT INTO t_json_string_time SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + number * 3600 FROM numbers(24);
CREATE TABLE t_format_row_time (dt DateTime) ENGINE = MergeTree ORDER BY formatRowNoNewline('TSV', dt) SETTINGS index_granularity = 1;
INSERT INTO t_format_row_time SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + number * 3600 FROM numbers(24);
CREATE TABLE t_time_zone (s String) ENGINE = MergeTree ORDER BY toDateTime(s) SETTINGS index_granularity = 1;
INSERT INTO t_time_zone SELECT toString(toDateTime('2024-01-01 00:00:00', 'UTC') + number * 3600) FROM numbers(48);

-- A setting that the function does not declare keeps the key usable, e.g. a format setting for `toUInt64`.
CREATE TABLE t_unrelated_setting (s String) ENGINE = MergeTree ORDER BY toUInt64(s);
INSERT INTO t_unrelated_setting SELECT toString(number) FROM numbers(1000);
SELECT 'unrelated setting reads in order', countIf(explain LIKE '%Read type: InOrder%')
FROM (EXPLAIN PLAN actions = 1 SELECT s FROM t_unrelated_setting ORDER BY toUInt64(s) LIMIT 10
      SETTINGS optimize_read_in_order = 1, output_format_csv_crlf_end_of_line = 1);

SELECT 'declared', name, result_settings FROM system.functions WHERE name IN ('locate', 'DATE_FORMAT') ORDER BY name;

SELECT 'same session';
SELECT 'primary key', count() FROM t_sort WHERE formatDateTime(dt, '%M') = 'May' SETTINGS force_primary_key = 1;
SELECT 'locate primary key', count() FROM t_locate WHERE locate(s, 'b') = 1 SETTINGS force_primary_key = 1;
SELECT 'skip index', count() FROM t_locate_index WHERE locate(s, 'b') = 0 SETTINGS force_data_skipping_indices = 'idx';

SET formatdatetime_parsedatetime_m_is_month_name = 0, function_locate_has_mysql_compatible_argument_order = 0;
SELECT 'changed session';
SELECT 'primary key', count() FROM t_sort WHERE formatDateTime(dt, '%M') = '05';
SELECT 'primary key expected', count() FROM t_sort WHERE formatDateTime(dt, '%M') = '05' SETTINGS use_primary_key = 0;
SELECT 'primary key forced', count() FROM t_sort WHERE formatDateTime(dt, '%M') = '05' SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT 'shared prefix uses the index', count() FROM t_shared_prefix WHERE lower(s) = '5' SETTINGS force_primary_key = 1;
SELECT 'distinct in order', count() FROM (SELECT DISTINCT b, formatDateTime(dt, '%M') FROM t_sort) SETTINGS optimize_distinct_in_order = 1, optimize_read_in_order = 1;
SELECT 'read in order', groupArray(k) = arraySort(groupArray(k)) FROM (SELECT formatDateTime(dt, '%M') AS k FROM t_sort ORDER BY k SETTINGS optimize_read_in_order = 1, max_threads = 1);
SELECT 'locate primary key', count() FROM t_locate WHERE locate(s, 'b') = 0;
SELECT 'locate primary key expected', count() FROM t_locate WHERE locate(s, 'b') = 0 SETTINGS use_primary_key = 0;
SELECT 'skip index', count() FROM t_locate_index WHERE locate(s, 'b') = 2;
SELECT 'skip index expected', count() FROM t_locate_index WHERE locate(s, 'b') = 2 SETTINGS use_skip_indexes = 0;
SELECT 'nested sample', count() FROM t_nested_sample SAMPLE 0.5;
SELECT 'nested sample without primary key', count() FROM t_nested_sample SAMPLE 0.5 SETTINGS use_primary_key = 0;
SELECT 'nested sample expected', count() FROM t_nested_sample WHERE cityHash64(s, upper(formatDateTime(toDateTime('2024-05-01 00:05:00', 'UTC'), '%M'))) < 9223372036854775808;
SELECT 'lambda primary key', count() FROM t_lambda WHERE arrayMap(x -> formatDateTime(x, '%M'), [dt]) = ['05'];
SELECT 'lambda primary key expected', count() FROM t_lambda WHERE arrayMap(x -> formatDateTime(x, '%M'), [dt]) = ['05'] SETTINGS use_primary_key = 0;
SELECT 'partition filter', count() FROM t_part WHERE formatDateTime(dt, '%M') = '05' SETTINGS optimize_use_implicit_projections = 0;
SELECT 'partition filter expected', count() FROM t_part WHERE formatDateTime(dt, '%M') = '05'
SETTINGS use_partition_pruning = 0, use_primary_key = 0, optimize_use_implicit_projections = 0;
SELECT 'groups per partition', count() FROM (SELECT formatDateTime(dt, '%M') AS k FROM t_part GROUP BY k)
SETTINGS allow_aggregate_partitions_independently = 1, force_aggregate_partitions_independently = 1, optimize_use_implicit_projections = 0;
SELECT 'groups expected', count() FROM (SELECT formatDateTime(dt, '%M') AS k FROM t_part GROUP BY k)
SETTINGS allow_aggregate_partitions_independently = 0, optimize_use_implicit_projections = 0;

-- The full scan runs first: the query condition cache would otherwise give it the granules the query with the index selected.
SET precise_float_parsing = 0;
SELECT 'json float expected', count() FROM t_json_float WHERE JSONExtractFloat(j, 'x') = 1.6724999999999999 SETTINGS use_primary_key = 0;
SELECT 'json float', count() FROM t_json_float WHERE JSONExtractFloat(j, 'x') = 1.6724999999999999;
SET precise_float_parsing = 1;
SET date_time_overflow_behavior = 'throw';
SELECT 'json overflow expected', count() FROM t_json_overflow WHERE JSONExtract(s, 'DateTime') = toDateTime(0) SETTINGS use_primary_key = 0;
SELECT 'json overflow', count() FROM t_json_overflow WHERE JSONExtract(s, 'DateTime') = toDateTime(0);
SET date_time_overflow_behavior = 'ignore';
SET bool_true_representation = 'false', bool_false_representation = 'true';
SELECT 'bool cast expected', count() FROM t_bool_cast WHERE CAST(s AS Bool) = true SETTINGS use_primary_key = 0;
SELECT 'bool cast', count() FROM t_bool_cast WHERE CAST(s AS Bool) = true;
SET bool_true_representation = 'true', bool_false_representation = 'false';
SET input_format_try_infer_dates = 0;
SELECT 'json inference expected', count() FROM t_json_inference WHERE dynamicType(getSubcolumn(CAST(s AS JSON), 'a')) = 'DateTime'
SETTINGS use_primary_key = 0;
SELECT 'json inference', count() FROM t_json_inference WHERE dynamicType(getSubcolumn(CAST(s AS JSON), 'a')) = 'DateTime';
SET format_csv_delimiter = ';', type_json_skip_null_typed_paths = 1;
SELECT 'format row expected', count() FROM t_format_row WHERE formatRowNoNewline('CSV', x, y) = '0;5000' SETTINGS use_primary_key = 0;
SELECT 'format row', count() FROM t_format_row WHERE formatRowNoNewline('CSV', x, y) = '0;5000';
SELECT 'json string expected', count() FROM t_json_string WHERE toJSONString(j) = '{}' SETTINGS use_primary_key = 0;
SELECT 'json string', count() FROM t_json_string WHERE toJSONString(j) = '{}';
SELECT 'json to string expected', count() FROM t_json_to_string WHERE toString(j) = '{}' SETTINGS use_primary_key = 0;
SELECT 'json to string', count() FROM t_json_to_string WHERE toString(j) = '{}';
SELECT 'json raw quote expected', count() FROM t_json_raw_quote WHERE JSONExtractRaw(j, 'a') = '"1"'
SETTINGS output_format_json_quote_64bit_integers = 1, use_primary_key = 0;
SELECT 'json raw quote', count() FROM t_json_raw_quote WHERE JSONExtractRaw(j, 'a') = '"1"'
SETTINGS output_format_json_quote_64bit_integers = 1, type_json_skip_null_typed_paths = 0;
SELECT 'json text quote expected', count() FROM t_json_text_quote WHERE toString(j) = '{"a":"1"}'
SETTINGS output_format_json_quote_64bit_integers = 1, use_primary_key = 0;
SELECT 'json text quote', count() FROM t_json_text_quote WHERE toString(j) = '{"a":"1"}'
SETTINGS output_format_json_quote_64bit_integers = 1, type_json_skip_null_typed_paths = 0;
SELECT 'json parser expected', count() FROM t_json_parser WHERE JSONType(j, 'a') = 'Double'
SETTINGS allow_simdjson = 0, type_json_skip_null_typed_paths = 0, use_primary_key = 0;
SELECT 'json parser', count() FROM t_json_parser WHERE JSONType(j, 'a') = 'Double' SETTINGS allow_simdjson = 0, type_json_skip_null_typed_paths = 0;
SELECT 'json has expected', count() FROM t_json_has WHERE JSONHas(j, 'a') = 0 SETTINGS use_primary_key = 0;
SELECT 'json has', count() FROM t_json_has WHERE JSONHas(j, 'a') = 0;
SET session_timezone = 'Pacific/Kiritimati';
SELECT 'time zone expected', count() FROM t_time_zone WHERE toDateTime(s) = toDateTime('2024-01-01 05:00:00', 'UTC') SETTINGS use_primary_key = 0;
SELECT 'time zone', count() FROM t_time_zone WHERE toDateTime(s) = toDateTime('2024-01-01 05:00:00', 'UTC');
SELECT 'array time zone expected', count() FROM t_array_time_zone WHERE CAST(a AS Array(DateTime)) = [toDateTime('2024-01-01 05:00:00', 'UTC')]
SETTINGS use_primary_key = 0;
SELECT 'array time zone', count() FROM t_array_time_zone WHERE CAST(a AS Array(DateTime)) = [toDateTime('2024-01-01 05:00:00', 'UTC')];
SELECT 'date to time expected', count() FROM t_date_to_time WHERE toDateTime(d) = toDateTime('2024-01-05 00:00:00', 'Pacific/Kiritimati')
SETTINGS use_primary_key = 0;
SELECT 'date to time', count() FROM t_date_to_time WHERE toDateTime(d) = toDateTime('2024-01-05 00:00:00', 'Pacific/Kiritimati');
SELECT 'dynamic time expected', count() FROM t_dynamic_time WHERE toDateTime(d) = toDateTime('2024-01-01 05:00:00', 'UTC')
SETTINGS use_primary_key = 0;
SELECT 'dynamic time', count() FROM t_dynamic_time WHERE toDateTime(d) = toDateTime('2024-01-01 05:00:00', 'UTC');
SELECT 'time as text expected', count() FROM t_time_as_text WHERE toString(toDateTime(n)) = '2024-01-01 19:00:00' SETTINGS use_primary_key = 0;
SELECT 'time as text', count() FROM t_time_as_text WHERE toString(toDateTime(n)) = '2024-01-01 19:00:00';
SELECT 'time as text index expected', count() FROM t_time_as_text_index WHERE toString(toDateTime(n)) = '2024-01-01 19:00:00'
SETTINGS use_skip_indexes = 0;
SELECT 'time as text index', count() FROM t_time_as_text_index WHERE toString(toDateTime(n)) = '2024-01-01 19:00:00';
-- Only `session_timezone` differs from the settings of the table here.
SET input_format_try_infer_dates = 1, type_json_skip_null_typed_paths = 0;
SELECT 'json inferred time expected', count() FROM t_json_inferred_time
WHERE assumeNotNull(toUInt32(dynamicElement(getSubcolumn(CAST(s AS JSON), 'a'), 'DateTime'))) = 1704067200 SETTINGS use_primary_key = 0;
SELECT 'json inferred time', count() FROM t_json_inferred_time
WHERE assumeNotNull(toUInt32(dynamicElement(getSubcolumn(CAST(s AS JSON), 'a'), 'DateTime'))) = 1704067200;
SELECT 'time array text expected', count() FROM t_time_array_text WHERE toString([dt]) = '[''2024-01-01 19:00:00'']' SETTINGS use_primary_key = 0;
SELECT 'time array text', count() FROM t_time_array_text WHERE toString([dt]) = '[''2024-01-01 19:00:00'']';
SELECT 'time array column expected', count() FROM t_time_array_text WHERE dt = toDateTime(1704067200) SETTINGS use_primary_key = 0;
SELECT 'time array column', count() FROM t_time_array_text WHERE dt = toDateTime(1704067200);
SELECT 'time array column in', count() FROM t_time_array_text WHERE dt IN (toDateTime(1704067200), toDateTime(1704070800));
-- Splitting the ranges of the parts by the sorting key filters them by the key computed with the time zone of the session.
SELECT 'split ranges', count() FROM t_split_ranges WHERE dt = toDateTime(1704067200)
SETTINGS use_primary_key = 0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1,
    use_query_condition_cache = 0, max_threads = 4;
SELECT 'json string time expected', count() FROM t_json_string_time WHERE toJSONString(dt) = '"2024-01-01 19:00:00"' SETTINGS use_primary_key = 0;
SELECT 'json string time', count() FROM t_json_string_time WHERE toJSONString(dt) = '"2024-01-01 19:00:00"';
SELECT 'format row time expected', count() FROM t_format_row_time WHERE formatRowNoNewline('TSV', dt) = '2024-01-01 19:00:00'
SETTINGS use_primary_key = 0;
SELECT 'format row time', count() FROM t_format_row_time WHERE formatRowNoNewline('TSV', dt) = '2024-01-01 19:00:00'
SETTINGS format_csv_delimiter = ',';
SELECT 'dynamic time text expected', count() FROM t_dynamic_time_text WHERE toString(d) = '[''2024-01-01 19:00:00'']' SETTINGS use_primary_key = 0;
SELECT 'dynamic time text', count() FROM t_dynamic_time_text WHERE toString(d) = '[''2024-01-01 19:00:00'']';
SET optimize_time_filter_with_preimage = 0;
SELECT 'start of day expected', count() FROM t_start_of_day WHERE toStartOfDay(d) = toDateTime('2024-01-02 00:00:00', 'Pacific/Kiritimati')
SETTINGS use_primary_key = 0;
SELECT 'start of day', count() FROM t_start_of_day WHERE toStartOfDay(d) = toDateTime('2024-01-02 00:00:00', 'Pacific/Kiritimati');
SET optimize_time_filter_with_preimage = 1;
SET optimize_if_transform_strings_to_enum = 1, optimize_if_transform_const_strings_to_lowcardinality = 1,
    input_format_try_infer_datetimes_only_datetime64 = 1;
SELECT 'if of numbers uses the index', count() FROM t_if_numbers WHERE if(x >= 0, -x, x) = -5 SETTINGS force_primary_key = 1;
SELECT 'cast to a number uses the index', count() FROM t_cast_number WHERE CAST(s AS UInt64) = 5 SETTINGS force_primary_key = 1;
SELECT 'date of time uses the index', count() FROM t_date_of_time WHERE toDate(ts) = toDate('2024-01-05') SETTINGS force_primary_key = 1;
SELECT 'explicit time zone uses the index', count() FROM t_explicit_time_zone WHERE toDateTime(s, 'UTC') = toDateTime('2024-01-01 05:00:00', 'UTC')
SETTINGS force_primary_key = 1;
SELECT 'integer addition uses the index', count() FROM t_plus_int WHERE x + 1 = 5
SETTINGS decimal_check_overflow = 0, date_time_overflow_behavior = 'saturate', force_primary_key = 1;
SELECT 'text comparison in a lambda uses the index', count() FROM t_lambda_text WHERE mapContainsKey(fm, 'abc')
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_format_row;
DROP TABLE t_json_string;
DROP TABLE t_json_to_string;
DROP TABLE t_time_zone;
DROP TABLE t_date_of_time;
DROP TABLE t_explicit_time_zone;
DROP TABLE t_if_numbers;
DROP TABLE t_cast_number;
DROP TABLE t_json_inference;
DROP TABLE t_array_time_zone;
DROP TABLE t_date_to_time;
DROP TABLE t_dynamic_time;
DROP TABLE t_json_has;
DROP TABLE t_time_as_text;
DROP TABLE t_time_as_text_index;
DROP TABLE t_json_float;
DROP TABLE t_json_overflow;
DROP TABLE t_bool_cast;
DROP TABLE t_json_inferred_time;
DROP TABLE t_time_array_text;
DROP TABLE t_unrelated_setting;
DROP TABLE t_shared_prefix;
DROP TABLE t_lambda_text;
DROP TABLE t_plus_int;
DROP TABLE t_start_of_day;
DROP TABLE t_split_ranges;
DROP TABLE t_json_parser;
DROP TABLE t_json_raw_quote;
DROP TABLE t_json_text_quote;
DROP TABLE t_dynamic_time_text;
DROP TABLE t_json_string_time;
DROP TABLE t_format_row_time;
DROP TABLE t_sort;
DROP TABLE t_locate;
DROP TABLE t_locate_index;
DROP TABLE t_nested;
DROP TABLE t_nested_sample;
DROP TABLE t_lambda;
DROP TABLE t_part;
