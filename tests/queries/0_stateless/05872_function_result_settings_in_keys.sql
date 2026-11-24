-- A function such as `formatDateTime` or `locate` returns other values when the session changes one of its result settings.
-- The values stored with a table (sorting key, partition key, skip index, the implicit `minmax_count` projection) were
-- computed with the settings of the table, so a session that changes the setting must not use them.

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
DROP TABLE IF EXISTS t_time_zone;

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
CREATE TABLE t_time_zone (s String) ENGINE = MergeTree ORDER BY toDateTime(s) SETTINGS index_granularity = 1;
INSERT INTO t_time_zone SELECT toString(toDateTime('2024-01-01 00:00:00', 'UTC') + number * 3600) FROM numbers(48);

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
SELECT 'distinct in order', count() FROM (SELECT DISTINCT b, formatDateTime(dt, '%M') FROM t_sort) SETTINGS optimize_distinct_in_order = 1, optimize_read_in_order = 1;
SELECT 'read in order', groupArray(k) = arraySort(groupArray(k)) FROM (SELECT formatDateTime(dt, '%M') AS k FROM t_sort ORDER BY k SETTINGS optimize_read_in_order = 1, max_threads = 1);
SELECT 'locate primary key', count() FROM t_locate WHERE locate(s, 'b') = 0;
SELECT 'locate primary key expected', count() FROM t_locate WHERE locate(s, 'b') = 0 SETTINGS use_primary_key = 0;
SELECT 'locate min max', max(locate(s, 'b')) FROM t_locate SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SELECT 'locate min max expected', max(locate(s, 'b')) FROM t_locate SETTINGS optimize_use_projections = 0;
SELECT 'skip index', count() FROM t_locate_index WHERE locate(s, 'b') = 2;
SELECT 'skip index expected', count() FROM t_locate_index WHERE locate(s, 'b') = 2 SETTINGS use_skip_indexes = 0;
SELECT 'nested min max', min(concat(s, upper(formatDateTime(toDateTime('2024-05-01 00:05:00', 'UTC'), '%M')))) FROM t_nested
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SELECT 'nested sample', count() FROM t_nested_sample SAMPLE 0.5;
SELECT 'nested sample without primary key', count() FROM t_nested_sample SAMPLE 0.5 SETTINGS use_primary_key = 0;
SELECT 'nested sample expected', count() FROM t_nested_sample WHERE cityHash64(s, upper(formatDateTime(toDateTime('2024-05-01 00:05:00', 'UTC'), '%M'))) < 9223372036854775808;
SELECT 'lambda primary key', count() FROM t_lambda WHERE arrayMap(x -> formatDateTime(x, '%M'), [dt]) = ['05'];
SELECT 'lambda primary key expected', count() FROM t_lambda WHERE arrayMap(x -> formatDateTime(x, '%M'), [dt]) = ['05'] SETTINGS use_primary_key = 0;
SELECT 'partition filter', count() FROM t_part WHERE formatDateTime(dt, '%M') = '05';
SELECT 'partition filter expected', count() FROM t_part WHERE formatDateTime(dt, '%M') = '05'
SETTINGS use_partition_pruning = 0, use_primary_key = 0, optimize_use_implicit_projections = 0;
SELECT 'groups per partition', count() FROM (SELECT formatDateTime(dt, '%M') AS k FROM t_part GROUP BY k)
SETTINGS allow_aggregate_partitions_independently = 1, force_aggregate_partitions_independently = 1, optimize_use_implicit_projections = 0;
SELECT 'groups expected', count() FROM (SELECT formatDateTime(dt, '%M') AS k FROM t_part GROUP BY k)
SETTINGS allow_aggregate_partitions_independently = 0, optimize_use_implicit_projections = 0;

-- The full scan runs first: the query condition cache would otherwise give it the granules the query with the index selected.
SET format_csv_delimiter = ';', type_json_skip_null_typed_paths = 1;
SELECT 'format row expected', count() FROM t_format_row WHERE formatRowNoNewline('CSV', x, y) = '0;5000' SETTINGS use_primary_key = 0;
SELECT 'format row', count() FROM t_format_row WHERE formatRowNoNewline('CSV', x, y) = '0;5000';
SELECT 'json string expected', count() FROM t_json_string WHERE toJSONString(j) = '{}' SETTINGS use_primary_key = 0;
SELECT 'json string', count() FROM t_json_string WHERE toJSONString(j) = '{}';
SET session_timezone = 'Pacific/Kiritimati';
SELECT 'time zone expected', count() FROM t_time_zone WHERE toDateTime(s) = toDateTime('2024-01-01 05:00:00', 'UTC') SETTINGS use_primary_key = 0;
SELECT 'time zone', count() FROM t_time_zone WHERE toDateTime(s) = toDateTime('2024-01-01 05:00:00', 'UTC');

DROP TABLE t_format_row;
DROP TABLE t_json_string;
DROP TABLE t_time_zone;
DROP TABLE t_sort;
DROP TABLE t_locate;
DROP TABLE t_locate_index;
DROP TABLE t_nested;
DROP TABLE t_nested_sample;
DROP TABLE t_lambda;
DROP TABLE t_part;
