-- { echo }

-- A cast to JSON must keep the instant of a typed DateTime path when the path changes its type and time zone.
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime('UTC')), 'JSON(d DateTime64(0, ''Asia/Tokyo''))').d);
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d Nullable(DateTime64(3, 'UTC'))), 'JSON(d Nullable(DateTime(''Asia/Tokyo'')))').d);
-- The second 01:30 of the DST fall-back hour.
SELECT toUnixTimestamp(CAST('{"d":1793514600}'::JSON(d DateTime('America/New_York')), 'JSON(d DateTime64(0, ''America/New_York''))').d);
-- Without the setting, the typed path `t` reads the whole object, with the text of `t.d`.
SELECT tupleElement(CAST('{"t":{"d":"2026-01-01 00:00:00"}}'::JSON(`t.d` DateTime('Asia/Tokyo')), 'JSON(t Tuple(d String), `t.d` DateTime(''UTC''))').t, 'd') SETTINGS type_json_allow_duplicated_key_with_literal_and_nested_object = 0;

DROP TABLE IF EXISTS t_json_datetime_alter;
CREATE TABLE t_json_datetime_alter (j JSON(d DateTime('UTC'))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_datetime_alter VALUES ('{"d":"2026-01-01 00:00:00"}');
ALTER TABLE t_json_datetime_alter MODIFY COLUMN j JSON(d DateTime64(0, 'Asia/Tokyo')) SETTINGS mutations_sync = 2;
SELECT toUnixTimestamp(j.d) FROM t_json_datetime_alter;
DROP TABLE t_json_datetime_alter;
