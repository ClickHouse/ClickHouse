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

SELECT 'Variant, values of an alternative that cannot be converted';
SELECT CAST(x AS UInt8) FROM (SELECT [1]::Array(UInt8)::Variant(UInt64, Array(UInt8)) AS x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT CAST(x AS UInt8) FROM (SELECT if(number = 1, [1]::Array(UInt8)::Variant(UInt64, Array(UInt8)), number::Variant(UInt64, Array(UInt8))) AS x FROM numbers(3)) FORMAT Null; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT CAST(x AS FixedString(2)) FROM (SELECT 5::UInt64::Variant(String, UInt64) AS x); -- { serverError NOT_IMPLEMENTED }

SELECT 'Variant, no alternative can be converted';
SELECT CAST(x AS UInt8) FROM (SELECT [1]::Array(UInt8)::Variant(Array(UInt8), Map(String, UInt8)) AS x LIMIT 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT CAST(x AS Nullable(UInt8)) FROM (SELECT NULL::Variant(Array(UInt8), Map(String, UInt8)) AS x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

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

SELECT 'ALTER';
SET mutations_sync = 2;
DROP TABLE IF EXISTS t_alter;
CREATE TABLE t_alter (id UInt64, v Variant(UInt64, Array(UInt8))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alter SELECT 1, 5::UInt64::Variant(UInt64, Array(UInt8));
ALTER TABLE t_alter MODIFY COLUMN v UInt8;
SELECT id, v, toTypeName(v) FROM t_alter;
DROP TABLE t_alter;
CREATE TABLE t_alter (id UInt64, v Variant(Array(UInt8), Map(String, UInt8))) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_alter MODIFY COLUMN v UInt8; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP TABLE t_alter;
