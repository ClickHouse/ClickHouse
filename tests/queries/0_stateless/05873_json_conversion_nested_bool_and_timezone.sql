-- A CAST to Bool must store only 0 or 1: equals treats Bool and plain UInt8 as the same type, so an
-- identity shortcut would reuse a UInt8 column with an out-of-range byte unchanged. This must hold at any
-- nesting, not only at the top level. JSON text hides the stored byte, so read it back with toUInt8.
SET session_timezone = 'UTC';

SELECT '-- standalone nested UInt8 -> Bool CAST normalizes the byte';
SELECT toUInt8(CAST(42::UInt8, 'Bool'));
SELECT arrayMap(x -> toUInt8(x), CAST([42, 0]::Array(UInt8), 'Array(Bool)'));
SELECT toUInt8(CAST(tuple(42::UInt8), 'Tuple(Bool)').1);
SELECT toUInt8(CAST(map('k', 42::UInt8), 'Map(String, Bool)')['k']);
SELECT toUInt8(assumeNotNull(CAST(42::Nullable(UInt8), 'Nullable(Bool)')));
SELECT arrayMap(x -> toUInt8(x), CAST([[42]]::Array(Array(UInt8)), 'Array(Array(Bool))')[1]);
SELECT toUInt8(CAST(42::UInt8, 'LowCardinality(Bool)')) SETTINGS allow_suspicious_low_cardinality_types = 1;

SELECT '-- a zone-only or domain-only change keeps the values';
SELECT toUnixTimestamp64Milli(CAST(toDateTime64('2026-01-01 00:00:00.123', 3, 'UTC'), 'DateTime64(3, ''Asia/Tokyo'')'));
SELECT arrayMap(x -> toUnixTimestamp64Milli(x), CAST([toDateTime64('2026-01-01 00:00:00.123', 3, 'UTC')], 'Array(DateTime64(3, ''Asia/Tokyo''))'));
SELECT arrayMap(x -> toUnixTimestamp(x), CAST([toDateTime('2026-01-01 00:00:00', 'UTC')], 'Array(DateTime(''Asia/Tokyo''))'));
SELECT toUnixTimestamp64Milli(assumeNotNull(accurateCastOrNull(toDateTime64('2026-01-01 00:00:00.123', 3, 'UTC'), 'DateTime64(3, ''Asia/Tokyo'')')));
SELECT CAST([(1.5, 2.5)]::Array(Point), 'Array(Tuple(Float64, Float64))') AS a, toTypeName(a);
SELECT CAST([(1.5, 2.5)]::Array(Tuple(Float64, Float64)), 'Array(Point)') AS a, toTypeName(a);

SELECT '-- JSON typed path UInt8 -> Bool normalizes, alone and with another path change';
SELECT toUInt8(CAST('{"a": 42}'::JSON(a UInt8), 'JSON(a Bool)').a);
SELECT toUInt8(CAST('{"a": 42, "b": 1}'::JSON(a UInt8, b Int32), 'JSON(a Bool, b Int64)').a);
SELECT arrayMap(x -> toUInt8(x), CAST('{"a": [42, 0], "b": 1}'::JSON(a Array(UInt8), b Int32), 'JSON(a Array(Bool), b Int64)').a);

-- A DateTime/DateTime64 zone-only change keeps the stored instant, as a plain CAST does, even when another
-- typed path changes too (issue 133915). The instant is zone-independent, so toUnixTimestamp is stable.
-- Wall clock 2026-01-01 00:00:00 is 1767225600 in UTC.
SELECT '-- zone-only DateTime change keeps the instant with another path change';
SELECT toUnixTimestamp(j.d), toUnixTimestamp(j.e), toUnixTimestamp64Milli(j.f)
FROM (SELECT CAST('{"d":"2026-01-01 00:00:00","e":"2026-01-01 00:00:00","f":"2026-01-01 00:00:00","b":1}'::JSON(d DateTime('UTC'), e DateTime('UTC'), f DateTime64(3, 'UTC'), b Int32),
                  'JSON(d DateTime(''Asia/Tokyo''), e DateTime64(0, ''Asia/Tokyo''), f DateTime64(3, ''Asia/Tokyo''), b Int64)') AS j);

-- A zone-only change inside a structural carrier (Tuple, nested JSON) also keeps the instant: the whole
-- object no longer falls back to a text round trip when another typed path changes.
SELECT '-- zone-only change inside a carrier keeps the instant';
SELECT toUnixTimestamp(j.t.1), toUnixTimestamp(j.n.d)
FROM (SELECT CAST('{"t":["2026-01-01 00:00:00"],"n":{"d":"2026-01-01 00:00:00"},"b":1}'::JSON(t Tuple(DateTime('UTC')), n JSON(d DateTime('UTC')), b Int32),
                  'JSON(t Tuple(DateTime(''Asia/Tokyo'')), n JSON(d DateTime(''Asia/Tokyo'')), b Int64)') AS j);
SELECT toUnixTimestamp(CAST('{"t":["2026-01-01 00:00:00"]}'::JSON(t Tuple(DateTime('UTC'))), 'JSON(t Tuple(DateTime(''Asia/Tokyo'')))').t.1);

SELECT '-- ALTER MODIFY COLUMN on MergeTree keeps the instant and normalizes Bool';
DROP TABLE IF EXISTS t_json_nested_bool_tz;
CREATE TABLE t_json_nested_bool_tz (j JSON(d DateTime('UTC'), a UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_nested_bool_tz VALUES ('{"d":"2026-01-01 00:00:00","a":42}');
ALTER TABLE t_json_nested_bool_tz MODIFY COLUMN j JSON(d DateTime('Asia/Tokyo'), a Bool, b Int32) SETTINGS mutations_sync = 2;
SELECT toUnixTimestamp(j.d), toUInt8(j.a) FROM t_json_nested_bool_tz;
DROP TABLE t_json_nested_bool_tz;
