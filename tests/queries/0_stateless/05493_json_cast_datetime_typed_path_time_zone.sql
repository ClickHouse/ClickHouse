-- { echo }

-- A cast to JSON must keep the instant of a typed DateTime path when the path changes its type and time zone.
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime('UTC')), 'JSON(d DateTime64(0, ''Asia/Tokyo''))').d);
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime64(0, 'UTC')), 'JSON(d DateTime(''Asia/Tokyo''))').d);
SELECT toUnixTimestamp64Milli(CAST('{"d":"2026-01-01 00:00:00.123"}'::JSON(d DateTime64(3, 'UTC')), 'JSON(d DateTime64(6, ''Asia/Tokyo''))').d);
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00","x":1}'::JSON(d DateTime('UTC'), x Int32), 'JSON(d DateTime(''Asia/Tokyo''), x Int64)').d);
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d Nullable(DateTime('UTC'))), 'JSON(d Nullable(DateTime64(0, ''Asia/Tokyo'')))').d);
SELECT arrayMap(x -> toUnixTimestamp(x), CAST('{"d":["2026-01-01 00:00:00"]}'::JSON(d Array(DateTime('UTC'))), 'JSON(d Array(DateTime64(0, ''Asia/Tokyo'')))').d);
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime), 'JSON(d DateTime64(0, ''Asia/Tokyo''))').d) SETTINGS session_timezone = 'America/New_York';

-- The second 01:30 of the DST fall-back hour.
SELECT toUnixTimestamp(CAST('{"d":1793514600}'::JSON(d DateTime('America/New_York')), 'JSON(d DateTime64(0, ''America/New_York''))').d);

SELECT toUnixTimestamp(CAST(CAST(tuple(toDateTime('2026-01-01 00:00:00', 'UTC')), 'Tuple(d DateTime(''UTC''))'), 'JSON(d DateTime64(0, ''Asia/Tokyo''))').d);
SELECT toUnixTimestamp(CAST(CAST(tuple(tuple(toDateTime('2026-01-01 00:00:00', 'UTC'))), 'Tuple(a Tuple(b DateTime(''UTC'')))'), 'JSON(a.b DateTime64(0, ''Asia/Tokyo''))').a.b);
SELECT toUnixTimestamp(CAST(map('d', toDateTime('2026-01-01 00:00:00', 'UTC')), 'JSON(d DateTime64(0, ''Asia/Tokyo''))').d);
SELECT toUnixTimestamp(CAST(CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime('UTC')), 'Dynamic'), 'JSON(d DateTime64(0, ''Asia/Tokyo''))').d);
SELECT toUnixTimestamp(tupleElement(CAST('{"t":{"d":"2026-01-01 00:00:00"}}'::JSON(t Tuple(d DateTime('UTC'))), 'JSON(t Tuple(d DateTime64(0, ''Asia/Tokyo'')))').t, 'd'));
SELECT arrayMap(x -> toUnixTimestamp(x.ts), CAST('{"e":[{"ts":1793514600,"n":"a"}],"x":1}'::JSON(e Array(Tuple(ts DateTime('America/New_York'), n String)), x Int32), 'JSON(e Array(Tuple(ts DateTime(''America/New_York''), n String)), x Int64)').e);
SELECT toUnixTimestamp(CAST('{"m":{"k":"2026-01-01 00:00:00"}}'::JSON(m Map(String, DateTime('UTC'))), 'JSON(m Map(String, DateTime64(0, ''Asia/Tokyo'')))').m['k']);
SELECT toUnixTimestamp(CAST(CAST(tuple(tuple(toDateTime('2026-01-01 00:00:00', 'UTC'))), 'Tuple(a Nullable(Tuple(b DateTime(''UTC''))))'), 'JSON(a.b DateTime64(0, ''Asia/Tokyo''))').a.b);
SELECT toUnixTimestamp(CAST(map('a', CAST(tuple(toDateTime('2026-01-01 00:00:00', 'UTC')), 'Tuple(b DateTime(''UTC''))')), 'JSON(a.b DateTime64(0, ''Asia/Tokyo''))').a.b);
SELECT toUnixTimestamp(CAST(CAST(tuple(toDateTime('2026-01-01 00:00:00', 'UTC')), 'Tuple(`a.b` DateTime(''UTC''))'), 'JSON(`a%2Eb` DateTime64(0, ''Asia/Tokyo''))').`a%2Eb`) SETTINGS json_type_escape_dots_in_keys = 1;
SELECT toUnixTimestamp(accurateCastOrNull('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime('UTC')), 'JSON(d DateTime64(0, ''Asia/Tokyo''))').d);
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d LowCardinality(Nullable(DateTime('UTC')))), 'JSON(d Nullable(DateTime64(0, ''Asia/Tokyo'')))').d) SETTINGS allow_suspicious_low_cardinality_types = 1;
SELECT toUnixTimestamp(CAST(CAST(map('d', toDateTime('2026-01-01 00:00:00', 'UTC')), 'Dynamic'), 'JSON(d DateTime64(0, ''Asia/Tokyo''))').d);
SELECT toUnixTimestamp(CAST(CAST(CAST(tuple(toDateTime('2026-01-01 00:00:00', 'UTC')), 'Tuple(d DateTime(''UTC''))'), 'Dynamic'), 'JSON(d DateTime64(0, ''Asia/Tokyo''))').d);
SELECT toUnixTimestamp(CAST('{"t":{"d":"2026-01-01 00:00:00"}}'::JSON(t Tuple(d DateTime('UTC'))), 'JSON(t Map(String, DateTime64(0, ''Asia/Tokyo'')))').t['d']);
SELECT toUnixTimestamp(tupleElement(CAST('{"m":{"d":"2026-01-01 00:00:00"}}'::JSON(m Map(String, DateTime('UTC'))), 'JSON(m Tuple(d DateTime64(0, ''Asia/Tokyo'')))').m, 'd'));
SELECT toUnixTimestamp(tupleElement(CAST('{"a":["2026-01-01 00:00:00"]}'::JSON(a Array(DateTime('UTC'))), 'JSON(a Tuple(DateTime64(0, ''Asia/Tokyo'')))').a, 1));
SELECT arrayMap(x -> toUnixTimestamp(x.2), CAST('{"m":{"k":"2026-01-01 00:00:00"}}'::JSON(m Map(String, DateTime('UTC'))), 'JSON(m Array(Tuple(String, DateTime64(0, ''Asia/Tokyo''))))').m) SETTINGS output_format_json_map_as_array_of_tuples = 1;

-- An object at a typed path that prefixes other typed paths fills those.
SELECT toUnixTimestamp(CAST('{"t":{"d":"2026-01-01 00:00:00"}}'::JSON(t Tuple(d DateTime('UTC'))), 'JSON(t Tuple(d DateTime64(0, ''Asia/Tokyo'')), `t.d` DateTime(''Asia/Tokyo''))').`t.d`);
SELECT toUnixTimestamp(CAST('{"t":{"d":"2026-01-01 00:00:00"}}'::JSON(t Tuple(d DateTime('Asia/Tokyo'))), 'JSON(t Tuple(d DateTime64(0, ''Asia/Tokyo'')), `t.d` DateTime(''Asia/Tokyo''))').`t.d`);
SELECT toUnixTimestamp(tupleElement(CAST('{"t":{"d":"2026-01-01 00:00:00"}}'::JSON(t Tuple(d DateTime('UTC'))), 'JSON(t Tuple(d DateTime64(0, ''Asia/Tokyo'')), `t.d` DateTime(''Asia/Tokyo''))').t, 'd')) SETTINGS type_json_allow_duplicated_key_with_literal_and_nested_object = 0;
-- Without the setting, the typed path `t` reads the whole object.
SELECT tupleElement(CAST('{"t":{"d":"2026-01-01 00:00:00"}}'::JSON(t Tuple(d DateTime('Asia/Tokyo'))), 'JSON(t Tuple(d String), `t.d` DateTime(''Asia/Tokyo''))').t, 'd') SETTINGS type_json_allow_duplicated_key_with_literal_and_nested_object = 0;
SELECT tupleElement(CAST('{"t":{"d":"2026-01-01 00:00:00"}}'::JSON(`t.d` DateTime('Asia/Tokyo')), 'JSON(t Tuple(d String), `t.d` DateTime(''Asia/Tokyo''))').t, 'd') SETTINGS type_json_allow_duplicated_key_with_literal_and_nested_object = 0;

-- Values that do not feed a `DateTime` keep their text.
SELECT CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime('Asia/Tokyo')), 'JSON(d String)').d;
SELECT toUnixTimestamp(CAST(CAST(tuple('2026-01-01 00:00:00'), 'Tuple(d String)'), 'JSON(d DateTime(''Asia/Tokyo''))').d);
SELECT CAST('{"d":"2026-01-01 00:00:00","x":"2026-01-01 00:00:00"}'::JSON(d DateTime('Asia/Tokyo'), x DateTime('UTC')), 'JSON(x DateTime64(0, ''Asia/Tokyo''))').d;
SELECT CAST('{"d":"2026-01-01 00:00:00","x":"2026-01-01 00:00:00"}'::JSON(x DateTime('UTC')), 'JSON(x DateTime64(0, ''Asia/Tokyo''))').d;
SELECT tupleElement(CAST(CAST(tuple(tuple(NULL, toDateTime('2026-01-01 00:00:00', 'Asia/Tokyo'))), 'Tuple(t Tuple(a Nullable(String), b DateTime(''Asia/Tokyo'')))'), 'JSON(t Tuple(String, DateTime64(0, ''Asia/Tokyo'')))').t, 1) SETTINGS output_format_json_skip_null_value_in_named_tuples = 1;

DROP TABLE IF EXISTS t_json_datetime_alter;
CREATE TABLE t_json_datetime_alter (id UInt8, j JSON(d DateTime('UTC'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_datetime_alter VALUES (1, '{"d":"2026-01-01 00:00:00"}');
ALTER TABLE t_json_datetime_alter MODIFY COLUMN j JSON(d DateTime64(0, 'Asia/Tokyo')) SETTINGS mutations_sync = 2;
SELECT toUnixTimestamp(j.d), j.d FROM t_json_datetime_alter;
DROP TABLE t_json_datetime_alter;

-- A part not yet rewritten by the ALTER is converted on read.
DROP TABLE IF EXISTS t_json_datetime_alter_pending;
CREATE TABLE t_json_datetime_alter_pending (id UInt8, j JSON(d DateTime('UTC'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_datetime_alter_pending VALUES (1, '{"d":"2026-01-01 00:00:00"}');
SYSTEM STOP MERGES t_json_datetime_alter_pending;
ALTER TABLE t_json_datetime_alter_pending MODIFY COLUMN j JSON(d DateTime64(0, 'Asia/Tokyo')) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT j FROM t_json_datetime_alter_pending;
DROP TABLE t_json_datetime_alter_pending;

-- The Map column type was created under UTC, the cast runs under Asia/Tokyo.
SET session_timezone = 'UTC';
DROP TABLE IF EXISTS t_json_datetime_map_session;
CREATE TABLE t_json_datetime_map_session (m Map(String, DateTime)) ENGINE = Memory;
INSERT INTO t_json_datetime_map_session VALUES (map('k', toDateTime(1767225600)));
SELECT toUnixTimestamp(CAST(m, 'JSON(k DateTime(''Asia/Tokyo''))').k), toUnixTimestamp(CAST(m, 'JSON(k DateTime(''UTC''))').k) FROM t_json_datetime_map_session SETTINGS session_timezone = 'Asia/Tokyo';
DROP TABLE t_json_datetime_map_session;
