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
