-- Functions of two geometries convert a constant argument once instead of once per row. The results must be
-- the same as with the argument materialized. The constant polygon is counterclockwise and not closed, so it
-- has to be corrected before it is used, and several rows use it.
-- https://github.com/ClickHouse/ClickHouse/issues/126446

DROP TABLE IF EXISTS polygons;

CREATE TABLE polygons (id UInt8, p Array(Array(Tuple(Float64, Float64)))) ENGINE = Memory;

-- Without rows, the constant is not converted, as before, so an invalid value is not an error.
SELECT count() FROM (SELECT polygonsWithinCartesian(p, [[(nan, 0.), (1., 0.), (1., 1.), (nan, 0.)]]) FROM polygons);

-- Clockwise and counterclockwise, closed and not closed.
INSERT INTO polygons VALUES
    (1, [[(0., 0.), (0., 1.), (1., 1.), (1., 0.), (0., 0.)]]),
    (2, [[(1., 1.), (3., 1.), (3., 3.), (1., 3.)]]),
    (3, [[(5., 5.), (5., 6.), (6., 6.), (6., 5.), (5., 5.)]]),
    (4, [[(-1., -1.), (-1., 3.), (3., 3.), (3., -1.)]]),
    (5, [[(0., 0.), (0., 2.), (2., 2.), (2., 0.), (0., 0.)]]);

SELECT polygonsWithinCartesian(p, [[(nan, 0.), (1., 0.), (1., 1.), (nan, 0.)]]) FROM polygons; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'polygonsWithinCartesian';
WITH [[(0., 0.), (2., 0.), (2., 2.), (0., 2.)]] AS c
SELECT id, polygonsWithinCartesian(p, c) AS a, a = polygonsWithinCartesian(p, materialize(c)),
    polygonsWithinCartesian(c, p) AS b, b = polygonsWithinCartesian(materialize(c), p)
FROM polygons ORDER BY id;

SELECT 'polygonsDistanceCartesian';
WITH [[(0., 0.), (2., 0.), (2., 2.), (0., 2.)]] AS c
SELECT id, polygonsDistanceCartesian(p, c) AS a, a = polygonsDistanceCartesian(p, materialize(c)),
    polygonsDistanceCartesian(c, p) AS b, b = polygonsDistanceCartesian(materialize(c), p)
FROM polygons ORDER BY id;

SELECT 'polygonsEqualsCartesian';
WITH [[(0., 0.), (2., 0.), (2., 2.), (0., 2.)]] AS c
SELECT id, polygonsEqualsCartesian(p, c) AS a, a = polygonsEqualsCartesian(p, materialize(c)),
    polygonsEqualsCartesian(c, p) AS b, b = polygonsEqualsCartesian(materialize(c), p)
FROM polygons ORDER BY id;

SELECT 'polygonsIntersectCartesian';
WITH [[(0., 0.), (2., 0.), (2., 2.), (0., 2.)]] AS c
SELECT id, polygonsIntersectCartesian(p, c) AS a, a = polygonsIntersectCartesian(p, materialize(c)),
    polygonsIntersectCartesian(c, p) AS b, b = polygonsIntersectCartesian(materialize(c), p)
FROM polygons ORDER BY id;

SELECT 'polygonsIntersectionCartesian';
WITH [[(0., 0.), (2., 0.), (2., 2.), (0., 2.)]] AS c
SELECT id, polygonsIntersectionCartesian(p, c) AS a, a = polygonsIntersectionCartesian(p, materialize(c)),
    polygonsIntersectionCartesian(c, p) AS b, b = polygonsIntersectionCartesian(materialize(c), p)
FROM polygons ORDER BY id;

SELECT 'polygonsSymDifferenceCartesian';
WITH [[(0., 0.), (2., 0.), (2., 2.), (0., 2.)]] AS c
SELECT id, polygonsSymDifferenceCartesian(p, c) AS a, a = polygonsSymDifferenceCartesian(p, materialize(c)),
    polygonsSymDifferenceCartesian(c, p) AS b, b = polygonsSymDifferenceCartesian(materialize(c), p)
FROM polygons ORDER BY id;

SELECT 'polygonsUnionCartesian';
WITH [[(0., 0.), (2., 0.), (2., 2.), (0., 2.)]] AS c
SELECT id, polygonsUnionCartesian(p, c) AS a, a = polygonsUnionCartesian(p, materialize(c)),
    polygonsUnionCartesian(c, p) AS b, b = polygonsUnionCartesian(materialize(c), p)
FROM polygons ORDER BY id;

SELECT 'geometryIntersectCartesian';
WITH [[(0., 0.), (2., 0.), (2., 2.), (0., 2.)]]::Polygon AS c
SELECT id, geometryIntersectCartesian(p::Polygon, c) AS a, a = geometryIntersectCartesian(p::Polygon, materialize(c)),
    geometryIntersectCartesian(c, p::Polygon) AS b, b = geometryIntersectCartesian(materialize(c), p::Polygon)
FROM polygons ORDER BY id;

SELECT 'a constant point and points with a constant polygon';
WITH [[(0., 0.), (2., 0.), (2., 2.), (0., 2.)]]::Polygon AS c
SELECT id, geometryIntersectCartesian((1., 1.)::Point, p::Polygon), geometryIntersectCartesian((id - 1., 1.)::Point, c)
FROM polygons ORDER BY id;

SELECT 'a constant multipolygon';
WITH [[[(0., 0.), (2., 0.), (2., 2.), (0., 2.)]], [[(5., 5.), (6., 5.), (6., 6.), (5., 6.)]]] AS c
SELECT id, polygonsIntersectionCartesian(p, c) AS a, a = polygonsIntersectionCartesian(p, materialize(c))
FROM polygons ORDER BY id;

SELECT 'Spherical';
WITH [[(0., 0.), (2., 0.), (2., 2.), (0., 2.)]] AS c
SELECT id,
    polygonsWithinSpherical(p, c) = polygonsWithinSpherical(p, materialize(c)),
    polygonsWithinSpherical(c, p) = polygonsWithinSpherical(materialize(c), p),
    polygonsDistanceSpherical(p, c) = polygonsDistanceSpherical(p, materialize(c)),
    polygonsDistanceSpherical(c, p) = polygonsDistanceSpherical(materialize(c), p),
    polygonsIntersectSpherical(p, c) = polygonsIntersectSpherical(p, materialize(c)),
    polygonsIntersectSpherical(c, p) = polygonsIntersectSpherical(materialize(c), p),
    polygonsIntersectionSpherical(p, c) = polygonsIntersectionSpherical(p, materialize(c)),
    polygonsIntersectionSpherical(c, p) = polygonsIntersectionSpherical(materialize(c), p),
    polygonsSymDifferenceSpherical(p, c) = polygonsSymDifferenceSpherical(p, materialize(c)),
    polygonsSymDifferenceSpherical(c, p) = polygonsSymDifferenceSpherical(materialize(c), p),
    polygonsUnionSpherical(p, c) = polygonsUnionSpherical(p, materialize(c)),
    polygonsUnionSpherical(c, p) = polygonsUnionSpherical(materialize(c), p),
    geometryIntersectSpherical(p::Polygon, c::Polygon) = geometryIntersectSpherical(p::Polygon, materialize(c::Polygon)),
    geometryIntersectSpherical(c::Polygon, p::Polygon) = geometryIntersectSpherical(materialize(c::Polygon), p::Polygon)
FROM polygons ORDER BY id;

-- A large constant polygon was copied, converted and corrected for every row, which took gigabytes of memory.
SELECT 'a large constant polygon';
SELECT countIf(polygonsWithinCartesian([[(x, 0.), (x + 0.001, 0.), (x + 0.001, 0.001), (x, 0.001)]],
    [arrayMap(i -> (cos(2 * pi() * i / 1000), sin(2 * pi() * i / 1000)), range(1000))]))
FROM (SELECT number / 20000 - 0.5 AS x FROM numbers(20000))
SETTINGS max_memory_usage = 100000000;

DROP TABLE polygons;
