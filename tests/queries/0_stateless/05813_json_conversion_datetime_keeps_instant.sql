-- A JSON conversion keeps the instant of every DateTime value, as a CAST does: the time zone only changes how
-- the value is shown. Wall clock 2026-01-01 00:00:00 is 1767225600 in UTC and 1767193200 in Asia/Tokyo.
SET session_timezone = 'UTC';

SELECT '-- typed path whose type changes';
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime('UTC')), 'JSON(d DateTime64(3, ''Asia/Tokyo''))').d);
SELECT toUnixTimestamp64Milli(CAST('{"d":"2026-01-01 00:00:00.123"}'::JSON(d DateTime64(3, 'Asia/Tokyo')), 'JSON(d DateTime64(6, ''UTC''))').d);
-- The second 01:30 of the daylight saving time fall-back hour.
SELECT toUnixTimestamp(CAST('{"d":1793514600}'::JSON(d DateTime('America/New_York')), 'JSON(d DateTime64(0, ''America/New_York''))').d);
SELECT arrayMap(x -> toUnixTimestamp(x), CAST('{"d":["2026-01-01 00:00:00"]}'::JSON(d Array(DateTime('Asia/Tokyo'))), 'JSON(d Array(DateTime64(0, ''UTC'')))').d);
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d LowCardinality(Nullable(DateTime('Asia/Tokyo')))), 'JSON(d DateTime64(0, ''UTC''))').d) SETTINGS allow_suspicious_low_cardinality_types = 1;

SELECT '-- a null becomes the destination default';
SELECT toUnixTimestamp(j.d) FROM (SELECT CAST(json::JSON(d Nullable(DateTime('Asia/Tokyo'))), 'JSON(d DateTime(''UTC''))') AS j FROM values('json String', ('{"d":"2026-01-01 00:00:00"}'), ('{"d":null}')));
SELECT arrayMap(x -> toUnixTimestamp(x), CAST('{"d":["2026-01-01 00:00:00", null]}'::JSON(d Array(Nullable(DateTime('Asia/Tokyo')))), 'JSON(d Array(DateTime64(0, ''UTC'')))').d);

SELECT '-- settings that change how a DateTime is written or read as text';
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime('UTC')), 'JSON(d DateTime64(3, ''Asia/Tokyo''))').d) SETTINGS type_json_skip_invalid_typed_paths = 1;
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime('UTC')), 'JSON(d DateTime64(3, ''Asia/Tokyo''))').d) SETTINGS date_time_output_format = 'iso';
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime('UTC')), 'JSON(d DateTime64(3, ''Asia/Tokyo''))').d) SETTINGS date_time_output_format = 'unix_timestamp', input_format_read_datetime_number_as_raw_value = 1;
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime('UTC')), 'JSON(d DateTime64(3, ''Asia/Tokyo''))').d) SETTINGS date_time_input_format = 'basic';

SELECT '-- a value outside the destination range saturates';
SELECT toUnixTimestamp(CAST('{"d":"2200-01-01 00:00:00"}'::JSON(d DateTime64(3, 'UTC')), 'JSON(d DateTime(''Asia/Tokyo''))').d);
SELECT toUnixTimestamp(CAST('{"d":"1960-01-01 00:00:00"}'::JSON(d DateTime64(3, 'UTC')), 'JSON(d DateTime(''Asia/Tokyo''))').d);

SELECT '-- into a String the wall clock of the source time zone, whatever date_time_output_format says';
SELECT CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime('Asia/Tokyo')), 'JSON(d String)').d SETTINGS date_time_output_format = 'iso';
SELECT CAST('{"d":"2026-01-01 00:00:00"}'::JSON, 'JSON(d String)').d SETTINGS date_time_output_format = 'iso';

SELECT '-- the text of a date and time is not a number or a date';
SELECT CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime('Asia/Tokyo')), 'JSON(d Int64)'); -- { serverError INCORRECT_DATA }
SELECT CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime), 'JSON(d Date)'); -- { serverError INCORRECT_DATA }

SELECT '-- typed path removed: the inferred type, without a time zone';
SELECT toUnixTimestamp(j.d::DateTime), dynamicType(j.d) FROM (SELECT CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime('Asia/Tokyo')), 'JSON') AS j);
SELECT toUnixTimestamp(j.d::DateTime), dynamicType(j.d) FROM (SELECT CAST('{"d":"2026-01-01 00:00:00"}'::JSON(max_dynamic_paths = 0, d DateTime('Asia/Tokyo')), 'JSON(max_dynamic_paths = 0)') AS j);
SELECT toUnixTimestamp64Milli(j.d::DateTime64(3)), dynamicType(j.d) FROM (SELECT CAST('{"d":"2026-01-01 00:00:00.123"}'::JSON(d DateTime64(3, 'Asia/Tokyo')), 'JSON') AS j);
SELECT arrayMap(x -> toUnixTimestamp(x), j.d::Array(DateTime)), dynamicType(j.d) FROM (SELECT CAST('{"d":["2026-01-01 00:00:00"]}'::JSON(d Array(DateTime('Asia/Tokyo'))), 'JSON') AS j);
SELECT toUnixTimestamp(j.d::DateTime), dynamicType(j.d) FROM (SELECT CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime('Asia/Tokyo')), 'JSON') AS j) SETTINGS date_time_output_format = 'unix_timestamp';
SELECT j.d, dynamicType(j.d) FROM (SELECT CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime('Asia/Tokyo')), 'JSON') AS j) SETTINGS input_format_try_infer_datetimes = 0;

SELECT '-- dynamic value into a new typed path';
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON, 'JSON(d DateTime(''Asia/Tokyo''))').d);
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00.123"}'::JSON, 'JSON(d DateTime(''Asia/Tokyo''))').d);
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON(max_dynamic_paths = 0), 'JSON(max_dynamic_paths = 0, d DateTime(''Asia/Tokyo''))').d);
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON(max_dynamic_types = 0), 'JSON(max_dynamic_types = 0, d DateTime(''Asia/Tokyo''))').d);
-- A string is parsed in the time zone of the destination.
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON, 'JSON(d DateTime(''Asia/Tokyo''))').d) SETTINGS input_format_try_infer_datetimes = 0;

SELECT '-- MODIFY COLUMN';
DROP TABLE IF EXISTS t_json_datetime_instant;
CREATE TABLE t_json_datetime_instant (j JSON(d DateTime('UTC'))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_datetime_instant VALUES ('{"d":"2026-01-01 00:00:00"}');
ALTER TABLE t_json_datetime_instant MODIFY COLUMN j JSON(d DateTime64(0, 'Asia/Tokyo')) SETTINGS mutations_sync = 2;
SELECT toUnixTimestamp(j.d) FROM t_json_datetime_instant;
DROP TABLE t_json_datetime_instant;
