-- A `Point`, `LineString` or `Polygon` is cast to `MultiPoint`, `MultiLineString` or `MultiPolygon`
-- as a collection of one element. https://github.com/ClickHouse/ClickHouse/issues/127111

SELECT (1., 2.)::Point::MultiPoint AS g, toTypeName(g);
SELECT [(0., 0.), (1., 1.)]::LineString::MultiLineString AS g, toTypeName(g);
SELECT [[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]::Polygon::MultiPolygon AS g, toTypeName(g);

-- Non-constant input, different sizes per row (row 0 is an empty polygon).
SELECT CAST(arrayMap(i -> [(toFloat64(i), 0.)], range(number))::Polygon AS MultiPolygon) FROM numbers(3);
SELECT CAST((toFloat64(number), 0.)::Point AS MultiPoint) FROM numbers(2);

-- Inside Array, Tuple, Map, Variant and Dynamic.
SELECT [[[(0., 0.)]], [[(1., 1.)], [(2., 2.)]]]::Array(Polygon)::Array(MultiPolygon);
SELECT tuple((1., 2.)::Point)::Tuple(MultiPoint);
SELECT map('a', [(0., 0.), (1., 1.)]::LineString)::Map(String, MultiLineString);
SELECT CAST([[(0., 0.)]]::Polygon::Dynamic AS MultiPolygon);

-- Nullable and Variant elements of an Array.
SELECT [(1., 2.)::Nullable(Point)]::Array(MultiPoint);
SELECT [NULL::Nullable(Point)]::Array(MultiPoint); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT CAST([[[(0., 0.)]]::Polygon::Variant(Polygon, MultiPolygon), [[[(1., 1.)]], [[(2., 2.)]]]::MultiPolygon::Variant(Polygon, MultiPolygon)] AS Array(MultiPolygon));
SELECT CAST([[[(0., 0.)]]::Polygon::Variant(Polygon, String)] AS Array(MultiPolygon)); -- { serverError TYPE_MISMATCH }
SELECT CAST([[1]::Variant(Array(UInt8), Array(String))] AS Array(Array(UInt8))); -- { serverError TYPE_MISMATCH }

DROP TABLE IF EXISTS t_geo_promotion_variant;
CREATE TABLE t_geo_promotion_variant (id UInt32, g Variant(Polygon, MultiPolygon)) ENGINE = Memory;
INSERT INTO t_geo_promotion_variant VALUES (1, [[(0., 0.)]]::Polygon), (2, [[[(1., 1.)]], [[(2., 2.)]]]::MultiPolygon);
SELECT id, CAST(g AS MultiPolygon) FROM t_geo_promotion_variant ORDER BY id;
DROP TABLE t_geo_promotion_variant;

-- Table columns: INSERT SELECT, an INSERT VALUES expression, and ALTER MODIFY COLUMN.
DROP TABLE IF EXISTS t_geo_promotion_src;
DROP TABLE IF EXISTS t_geo_promotion_dst;
CREATE TABLE t_geo_promotion_src (id UInt32, p Polygon) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_geo_promotion_src VALUES (1, [[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]), (2, []);
CREATE TABLE t_geo_promotion_dst (id UInt32, mp MultiPolygon) ENGINE = Memory;
INSERT INTO t_geo_promotion_dst SELECT id, p FROM t_geo_promotion_src;
INSERT INTO t_geo_promotion_dst VALUES (3, readWKTPolygon('POLYGON((0 0,2 0,2 2,0 0))'));
SELECT id, mp FROM t_geo_promotion_dst ORDER BY id;
ALTER TABLE t_geo_promotion_src MODIFY COLUMN p MultiPolygon SETTINGS mutations_sync = 2;
SELECT id, p, toTypeName(p) FROM t_geo_promotion_src ORDER BY id;
DROP TABLE t_geo_promotion_src;
DROP TABLE t_geo_promotion_dst;

-- Only these three promotions: no other geo type, no plain array, no demotion.
SELECT (1., 2.)::Point::Array(Point); -- { serverError TYPE_MISMATCH }
SELECT [[(0., 0.)]]::Array(Array(Tuple(Float64, Float64)))::MultiPolygon; -- { serverError TYPE_MISMATCH }
SELECT [[(0., 0.)]]::Array(Array(Tuple(Float64, Float64)))::Array(Array(Array(Tuple(Float64, Float64)))); -- { serverError TYPE_MISMATCH }
SELECT [(0., 0.)]::Ring::MultiLineString; -- { serverError TYPE_MISMATCH }
SELECT [[[(0., 0.)]]]::MultiPolygon::Polygon; -- { serverError TYPE_MISMATCH }
SELECT ['[(0, 0), (1, 1)]']::Array(String)::MultiLineString; -- { serverError TYPE_MISMATCH }
