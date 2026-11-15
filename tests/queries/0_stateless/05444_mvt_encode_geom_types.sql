-- Tags: no-fasttest
-- Tag no-fasttest: Ring, Polygon and MultiPolygon are encoded through the wagyu contrib, which is disabled in the fast-test (ENABLE_LIBRARIES=0) build.
-- Tests the per-geometry-type dispatch of MVTEncodeGeom: MultiPoint clipping, Ring and MultiLineString
-- inputs, every discriminator of the `Geometry` variant, and the limit on the `buffer` argument.
-- Expected behaviour: the FunctionDocumentation block of src/Functions/MVTEncodeGeom.cpp - the supported
-- input types are Point, MultiPoint, LineString, MultiLineString, Ring, Polygon, MultiPolygon and the
-- Geometry variant; `buffer` is in [0, 2147483647]; geometries falling entirely outside the tile become NULL.

SELECT '-- MVTEncodeGeom: a buffer beyond the representable MVT coordinate range is rejected';
-- clip = 0 (the seventh argument) so that only the buffer range check can reject this row:
-- the extent + 2 * buffer check applies to clipped geometries only.
SELECT MVTEncodeGeom((0.0, 0.0)::Point, 0, 0, 0, 4096, CAST(2147483648, 'UInt64'), 0); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT '-- MVTEncodeGeom: a MultiPoint keeps the points inside the tile and drops the rest';
SELECT MVTEncodeGeom([(13.37, 52.52), (0.0, 0.0)]::MultiPoint, 10, 550, 335);

SELECT '-- MVTEncodeGeom: a MultiPoint lying entirely outside the tile becomes NULL';
SELECT MVTEncodeGeom([(0.0, 0.0), (1.0, 1.0)]::MultiPoint, 10, 550, 335) IS NULL;

SELECT '-- MVTEncodeGeom: a MultiLineString is projected into tile-local pixel space';
SELECT MVTEncodeGeom([[(13.3, 52.4), (13.7, 52.6)]]::MultiLineString, 10, 550, 335);

SELECT '-- MVTEncodeGeom: a Ring is encoded as a polygon and clipped to the tile plus buffer';
SELECT wkt(MVTEncodeGeom([(13.3, 52.4), (13.7, 52.4), (13.7, 52.7), (13.3, 52.4)]::Ring, 10, 550, 335));

SELECT '-- MVTEncodeGeom: every discriminator of the Geometry variant is dispatched, a NULL inside the variant stays NULL';
SELECT p.1 AS geometry_type, wkt(MVTEncodeGeom(p.2, 2, 2, 1, 4096, 0)) AS encoded
FROM
(
    SELECT arrayJoin([
        ('Point', (13.0, 52.0)::Point::Geometry),
        ('LineString', [(13.0, 52.0), (14.0, 53.0)]::LineString::Geometry),
        ('MultiLineString', [[(13.0, 52.0), (14.0, 53.0)]]::MultiLineString::Geometry),
        ('MultiPoint', [(13.0, 52.0), (14.0, 53.0)]::MultiPoint::Geometry),
        ('Ring', [(13.0, 52.0), (14.0, 52.0), (14.0, 53.0), (13.0, 52.0)]::Ring::Geometry),
        ('Polygon', [[(13.0, 52.0), (14.0, 52.0), (14.0, 53.0), (13.0, 52.0)]]::Polygon::Geometry),
        ('MultiPolygon', [[[(13.0, 52.0), (14.0, 52.0), (14.0, 53.0), (13.0, 52.0)]]]::MultiPolygon::Geometry),
        ('NULL inside the variant', CAST(NULL, 'Geometry'))
    ]) AS p
)
ORDER BY geometry_type;

SELECT '-- MVTEncodeGeom: a NULL tile argument on a Geometry input yields NULL';
SELECT MVTEncodeGeom((13.37, 52.52)::Point::Geometry, CAST(NULL, 'Nullable(UInt8)'), 0, 0) IS NULL;
