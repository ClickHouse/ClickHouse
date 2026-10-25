-- `lengthCartesian` adds up the distances between consecutive points.
SELECT lengthCartesian([(0., 0.), (3., 4.), (3., 10.)]::LineString);
SELECT lengthCartesian([[(0., 0.), (3., 4.)], [(1., 1.), (1., 3.)]]::MultiLineString);
SELECT ST_Length(readWKTLineString('LINESTRING (0 0, 3 4)'));

-- An array of points without a type name has the type `Ring`, which is measured as a line.
SELECT lengthCartesian([(0., 0.), (3., 4.), (3., 10.)]);
SELECT lengthCartesian([(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]::Ring);

-- Lines without segments.
SELECT lengthCartesian([]::LineString), lengthCartesian([(1., 1.)]::LineString), lengthCartesian([]::MultiLineString);

-- Points and areas have no length.
SELECT
    lengthCartesian((1., 2.)::Point),
    lengthCartesian([(1., 2.), (3., 4.)]::MultiPoint),
    lengthCartesian([[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]::Polygon),
    lengthCartesian([[[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]]::MultiPolygon);

-- A `Geometry` column with a different type in each row.
SELECT wkt, lengthCartesian(readWKT(wkt)), round(lengthSpherical(readWKT(wkt)), 9), round(lengthSpheroid(readWKT(wkt)), 3)
FROM values('wkt String',
    'LINESTRING (0 0, 1 0)',
    'MULTILINESTRING ((0 0, 0 1), (10 0, 11 0))',
    'POLYGON ((0 0, 1 0, 1 1, 0 0))',
    'POINT (1 2)',
    'MULTIPOINT ((1 2), (3 4))');

-- A `NULL` geometry has no length.
SELECT lengthCartesian(g), round(lengthSpherical(g), 9), round(lengthSpheroid(g), 3)
FROM (SELECT arrayJoin([readWKT('LINESTRING (0 0, 3 4)'), NULL]) AS g);

-- `lengthSpherical` is an angle in radians: one degree along the equator, also across the antimeridian.
SELECT round(lengthSpherical([(0., 0.), (1., 0.)]::LineString), 9), round(lengthSpherical([(179.5, 0.), (-179.5, 0.)]::LineString), 9);

-- `lengthSpheroid` is in meters on WGS84. One degree along the equator is 6378137 * pi / 180 meters, and one degree of latitude
-- from the equator is 110574.389 meters. The shortest path between two points can cross the antimeridian.
SELECT
    round(lengthSpheroid([(0., 0.), (1., 0.)]::LineString), 3),
    round(lengthSpheroid([(0., 0.), (0., 1.)]::LineString), 3),
    round(lengthSpheroid([(179.5, 0.), (-179.5, 0.)]::LineString), 3);

-- Nearly antipodal points, where Vincenty's formulae do not converge (19936288.579 meters by GeographicLib), and antipodal
-- points on the equator, where the shortest path runs through the poles: twice the length of a meridian quadrant.
SELECT
    round(lengthSpheroid([(0., 0.), (179.5, 0.5)]::LineString), 3),
    round(lengthSpheroid([(0., 0.), (180., 0.)]::LineString), 3);

-- A `Ring` is measured as a line on the spheroid too.
SELECT round(lengthSpheroid([(0., 0.), (1., 0.), (1., 1.), (0., 0.)]::Ring), 3);

-- London, Paris, Berlin (1223596.207 meters by Vincenty's formulae), also from (latitude, longitude) with `flipCoordinates`.
SELECT
    round(lengthSpheroid(readWKTLineString('LINESTRING (-0.1276 51.5072, 2.3522 48.8566, 13.405 52.52)')), 3),
    round(lengthSpheroid(flipCoordinates(readWKT('LINESTRING (51.5072 -0.1276, 48.8566 2.3522, 52.52 13.405)'))), 3);

-- Latitudes below -45 degrees (Ushuaia - Santiago, and a line at 60 to 75 degrees south).
SELECT
    round(lengthSpheroid([(-68.3, -54.8), (-70.65, -33.45)]::LineString), 3),
    round(lengthSpheroid([(0., -60.), (10., -60.), (20., -75.)]::LineString), 3);

-- A NaN or infinite coordinate has no position on the spheroid.
SELECT
    lengthSpheroid([(0., nan), (1., 0.)]::LineString),
    lengthSpheroid([(nan, 0.), (1., 0.)]::LineString),
    lengthSpheroid([(inf, 0.), (1., 0.)]::LineString),
    lengthSpheroid([(0., -inf), (1., 0.)]::LineString),
    lengthSpheroid([[(0., 0.), (1., 0.)], [(0., 0.), (0., nan)]]::MultiLineString),
    lengthSpheroid([(0., 0.), (1., 0.), (1., inf), (0., 0.)]::Ring);

SELECT lengthCartesian(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT lengthSpheroid('LINESTRING (0 0, 1 1)'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
