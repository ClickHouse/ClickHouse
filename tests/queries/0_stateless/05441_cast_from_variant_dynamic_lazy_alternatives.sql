-- `CAST` from `Variant` and `Dynamic` fails only for alternatives that have values in the data.

SELECT 'Variant';
SELECT CAST(x AS UInt8) FROM (SELECT 1::UInt64::Variant(UInt64, Array(UInt8)) AS x);
SELECT CAST(x AS UInt8) FROM (SELECT NULL::Variant(UInt64, Array(UInt8)) AS x);
SELECT CAST(x AS Nullable(UInt8)) FROM (SELECT NULL::Variant(UInt64, Array(UInt8)) AS x);
SELECT accurateCast(x, 'UInt8') FROM (SELECT 1::UInt64::Variant(UInt64, Array(UInt8)) AS x);
SELECT number, CAST(x AS UInt8) FROM (SELECT number, if(number = 1, NULL, number)::Variant(UInt64, Array(UInt8)) AS x FROM numbers(3)) ORDER BY number;
SELECT CAST([x] AS Array(UInt8)), CAST(tuple(x) AS Tuple(UInt8)), CAST(map('k', x) AS Map(String, UInt8)) FROM (SELECT 1::UInt64::Variant(UInt64, Array(UInt8)) AS x);
SELECT CAST(x AS Array(UInt8)) FROM (SELECT [1, 2]::Array(UInt8)::Variant(UInt64, Array(UInt8)) AS x);
SELECT CAST(x AS FixedString(2)) FROM (SELECT 'ab'::Variant(String, UInt64) AS x);
SELECT CAST(x AS Tuple(UInt8, UInt8)) FROM (SELECT (1, 2)::Tuple(UInt8, UInt8)::Variant(Tuple(UInt8, UInt8), Tuple(UInt8)) AS x);
SELECT CAST(x AS FixedString(2)) FROM (SELECT 'ab'::Variant(String)::Variant(String, FixedString(4)) AS x) SETTINGS allow_suspicious_variant_types = 1;
SELECT CAST(x AS Date) FROM (SELECT toDate('2020-01-01')::Variant(Date, Decimal(9, 2)) AS x);

SELECT 'Variant, values of an alternative that cannot be converted';
SELECT CAST(x AS UInt8) FROM (SELECT [1]::Array(UInt8)::Variant(UInt64, Array(UInt8)) AS x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT CAST(x AS UInt8) FROM (SELECT if(number = 1, [1]::Array(UInt8)::Variant(UInt64, Array(UInt8)), number::Variant(UInt64, Array(UInt8))) AS x FROM numbers(3)) FORMAT Null; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT CAST(x AS FixedString(2)) FROM (SELECT 5::UInt64::Variant(String, UInt64) AS x); -- { serverError NOT_IMPLEMENTED }
SELECT CAST(x AS FixedString(2)) FROM (SELECT 'abcd'::FixedString(4)::Variant(String, FixedString(4)) AS x) SETTINGS allow_suspicious_variant_types = 1; -- { serverError TOO_LARGE_STRING_SIZE }
SELECT CAST(x AS Date) FROM (SELECT 1.5::Decimal(9, 2)::Variant(Date, Decimal(9, 2)) AS x); -- { serverError ILLEGAL_COLUMN }

SELECT 'Variant, no alternative can be converted';
SELECT CAST(x AS UInt8) FROM (SELECT materialize([1]::Array(UInt8)::Variant(Array(UInt8), Map(String, UInt8))) AS x LIMIT 0);
SELECT CAST(x AS Nullable(UInt8)) FROM (SELECT NULL::Variant(Array(UInt8), Map(String, UInt8)) AS x);
SELECT CAST(x AS UInt8) FROM (SELECT [1]::Array(UInt8)::Variant(Array(UInt8), Map(String, UInt8)) AS x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Geometry';
SELECT CAST(g AS MultiPolygon) FROM (SELECT [[[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]]::MultiPolygon::Geometry AS g);
DROP TABLE IF EXISTS t_geometry;
CREATE TABLE t_geometry (id UInt64, g Geometry) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_geometry SELECT 1, [[[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]]::MultiPolygon::Geometry;
INSERT INTO t_geometry SELECT 2, (1., 2.)::Point::Geometry;
SELECT id, CAST(g AS MultiPolygon) FROM t_geometry WHERE id = 1;
SELECT id, CAST(g AS MultiPolygon) FROM t_geometry ORDER BY id FORMAT Null; -- { serverError TYPE_MISMATCH }
DROP TABLE t_geometry;

SELECT 'Dynamic';
SELECT CAST(x::Dynamic AS UInt8) FROM (SELECT 1::UInt64::Variant(UInt64, Array(UInt8)) AS x);
SELECT toUInt8(x::Dynamic) FROM (SELECT 1::UInt64::Variant(UInt64, Array(UInt8)) AS x);
SELECT CAST(x::Dynamic AS Variant(UInt64, String)) FROM (SELECT 1::UInt64::Variant(UInt64, Array(UInt8)) AS x);
SELECT CAST([x::Dynamic] AS Array(UInt8)) FROM (SELECT 1::UInt64::Variant(UInt64, Array(UInt8)) AS x);
SELECT CAST(x::Dynamic AS UInt8) FROM (SELECT [1]::Array(UInt8)::Variant(UInt64, Array(UInt8)) AS x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP TABLE IF EXISTS t_dynamic;
CREATE TABLE t_dynamic (id UInt64, d Dynamic) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dynamic SELECT number, if(number = 1, number::Dynamic, [1]::Array(UInt8)::Dynamic) FROM numbers(1, 2);
SELECT id, CAST(d AS UInt8) FROM t_dynamic WHERE id = 1;
SELECT id, CAST(d AS UInt8) FROM t_dynamic ORDER BY id FORMAT Null; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP TABLE t_dynamic;

SELECT 'accurateCastOrNull';
SELECT accurateCastOrNull(tuple(x::Dynamic), 'Tuple(UInt8)') FROM (SELECT [1]::Array(UInt8)::Variant(UInt64, Array(UInt8)) AS x);
SELECT accurateCastOrDefault(tuple(x::Dynamic), 'Tuple(UInt8)', tuple(99::UInt8)) FROM (SELECT [1]::Array(UInt8)::Variant(UInt64, Array(UInt8)) AS x);
SELECT accurateCastOrNull(tuple(x::Dynamic), 'Tuple(UInt8)') FROM (SELECT arrayJoin([1::UInt64::Variant(UInt64, Array(UInt8)), [1]::Array(UInt8)::Variant(UInt64, Array(UInt8))]) AS x);

SELECT 'ALTER';
SET mutations_sync = 2;
DROP TABLE IF EXISTS t_alter;
CREATE TABLE t_alter (id UInt64, v Variant(UInt64, Array(UInt8))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alter SELECT 1, 5::UInt64::Variant(UInt64, Array(UInt8));
ALTER TABLE t_alter MODIFY COLUMN v UInt8;
SELECT id, v, toTypeName(v) FROM t_alter;
DROP TABLE t_alter;
