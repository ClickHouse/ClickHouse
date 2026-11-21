-- { echo }

-- A cast to JSON must keep the instant of a typed DateTime path when the path changes its type and time zone.
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d DateTime('UTC')), 'JSON(d DateTime64(0, ''Asia/Tokyo''))').d);
SELECT toUnixTimestamp(CAST('{"d":"2026-01-01 00:00:00"}'::JSON(d Nullable(DateTime64(3, 'UTC'))), 'JSON(d Nullable(DateTime(''Asia/Tokyo'')))').d);
-- The second 01:30 of the DST fall-back hour.
SELECT toUnixTimestamp(CAST('{"d":1793514600}'::JSON(d DateTime('America/New_York')), 'JSON(d DateTime64(0, ''America/New_York''))').d);
-- Also inside Array, Tuple, Map, Nested and JSON typed paths.
SELECT toUnixTimestamp(j.a[1]), toUnixTimestamp(tupleElement(j.t, 'd')), toUnixTimestamp(j.m['k']), toUnixTimestamp(j.n[1].d), toUnixTimestamp(j.o[1].d)
FROM (SELECT CAST('{"a":["2026-01-01 00:00:00"],"t":{"d":"2026-01-01 00:00:00"},"m":{"k":"2026-01-01 00:00:00"},"n":[{"d":"2026-01-01 00:00:00"}],"o":[{"d":"2026-01-01 00:00:00"}]}'::JSON(a Array(DateTime('UTC')), t Tuple(d DateTime('UTC')), m Map(String, DateTime('UTC')), n Nested(d DateTime('UTC')), o Array(JSON(d DateTime('UTC')))),
    'JSON(a Array(DateTime64(0, ''Asia/Tokyo'')), t Tuple(d DateTime64(0, ''Asia/Tokyo'')), m Map(String, DateTime64(0, ''Asia/Tokyo'')), n Nested(d DateTime(''Asia/Tokyo'')), o Array(JSON(d DateTime64(0, ''Asia/Tokyo''))))') AS j);
-- Without the setting, the typed path `t` reads the whole object, with the text of `t.d`.
SELECT tupleElement(CAST('{"t":{"d":"2026-01-01 00:00:00"}}'::JSON(`t.d` DateTime('Asia/Tokyo')), 'JSON(t Tuple(d String), `t.d` DateTime(''UTC''))').t, 'd') SETTINGS type_json_allow_duplicated_key_with_literal_and_nested_object = 0;
-- With it, the object at `t` fills the typed path `t.d`.
SELECT toUnixTimestamp(CAST('{"t":{"d":"2026-01-01 00:00:00"}}'::JSON(t Tuple(d DateTime('Asia/Tokyo'))), 'JSON(t Tuple(d DateTime(''UTC'')), `t.d` DateTime(''Asia/Tokyo''))').`t.d`);
-- Tuple elements are read by name.
SELECT toUnixTimestamp(tupleElement(CAST('{"t":{"a":"2026-01-01 00:00:00","b":"2026-01-01 00:00:00"}}'::JSON(t Tuple(a DateTime('Asia/Tokyo'), b String)), 'JSON(t Tuple(b DateTime(''UTC''), a DateTime(''Asia/Tokyo'')))').t, 'a'));
-- An element that does not pair keeps its text and does not hold back the ones that do.
SELECT toUnixTimestamp(tupleElement(c.t, 'd')), tupleElement(c.t, 'a'), tupleElement(c.t, 'b')
FROM (SELECT CAST('{"t":{"d":"2026-01-01 00:00:00","a":[1,2],"b":[3]}}'::JSON(t Tuple(d DateTime('UTC'), a Array(UInt8), b Dynamic)), 'JSON(t Tuple(d DateTime64(0, ''Asia/Tokyo''), a String, b Array(UInt8)))') AS c);

DROP TABLE IF EXISTS t_json_datetime_alter;
CREATE TABLE t_json_datetime_alter (j JSON(d DateTime('UTC'), t Tuple(d DateTime('UTC'), a Array(UInt8)))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_datetime_alter VALUES ('{"d":"2026-01-01 00:00:00","t":{"d":"2026-01-01 00:00:00","a":[1,2]}}');
ALTER TABLE t_json_datetime_alter MODIFY COLUMN j JSON(d DateTime64(0, 'Asia/Tokyo'), t Tuple(d DateTime64(0, 'Asia/Tokyo'), a String)) SETTINGS mutations_sync = 2;
SELECT toUnixTimestamp(j.d), toUnixTimestamp(tupleElement(j.t, 'd')), tupleElement(j.t, 'a') FROM t_json_datetime_alter;
DROP TABLE t_json_datetime_alter;

-- The DateTime of a Variant also reads the text of its String.
DROP TABLE IF EXISTS t_json_datetime_variant;
CREATE TABLE t_json_datetime_variant (j JSON(v Variant(DateTime('UTC'), String))) ENGINE = Memory;
INSERT INTO t_json_datetime_variant SETTINGS date_time_input_format = 'basic' VALUES ('{"v":"2026-01-01 00:00"}');
SELECT toUnixTimestamp(variantElement(CAST(j, 'JSON(v Variant(DateTime64(0, ''Asia/Tokyo''), String))').v, 'DateTime64(0, \'Asia/Tokyo\')')) FROM t_json_datetime_variant SETTINGS date_time_input_format = 'best_effort';
DROP TABLE t_json_datetime_variant;
