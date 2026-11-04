SELECT '-- polygonMinimumRotatedRectangleCartesian';

-- A rectangle that is not rotated is its own minimum rectangle; interior points and holes don't matter.
SELECT polygonMinimumRotatedRectangleCartesian([[(0., 0.), (0., 1.), (2., 1.), (2., 0.), (0., 0.)], [(0.5, 0.2), (1., 0.2), (1., 0.8), (0.5, 0.2)]]::Polygon);
SELECT polygonMinimumRotatedRectangleCartesian([(0., 0.), (2., 1.), (1., 0.5), (0., 1.), (2., 0.)]::MultiPoint);

-- A rectangle of 5 by 2.5 rotated by atan(3/4), with points inside it. The corners are exact.
SELECT polygonMinimumRotatedRectangleCartesian([(0., 0.), (4., 3.), (2.5, 5.), (-1.5, 2.), (1., 1.5), (2., 3.)]::MultiPoint) AS r, polygonAreaCartesian(r);
-- Without its corner (2.5, 5), the rectangle is the same, but the convex hull is not.
SELECT polygonMinimumRotatedRectangleCartesian(points), polygonConvexHullCartesian(points)
FROM (SELECT [(0., 0.), (4., 3.), (2., 3.), (-1.5, 2.), (1., 1.5)]::MultiPoint AS points);

-- Far from the origin, where the area of the convex hull must not cancel out.
SELECT arrayMap(p -> (p.1 - 1e10, p.2 - 1e10), polygonMinimumRotatedRectangleCartesian([(1e10, 1e10), (1e10 + 100, 1e10), (1e10 + 50, 1e10 + 1), (1e10 + 20, 1e10 + 0.3)]::MultiPoint)[1]);

-- Tiny and huge coordinates, where the products of coordinates would underflow or overflow: the result scales exactly.
SELECT
    k,
    countIf(polygonMinimumRotatedRectangleCartesian(arrayMap(p -> (p.1 * pow(2., k), p.2 * pow(2., k)), points)::MultiPoint)
        = [arrayMap(p -> (p.1 * pow(2., k), p.2 * pow(2., k)), polygonMinimumRotatedRectangleCartesian(points::MultiPoint)[1])]),
    countIf(polygonConvexHullCartesian(arrayMap(p -> (p.1 * pow(2., k), p.2 * pow(2., k)), points)::MultiPoint)
        = [arrayMap(p -> (p.1 * pow(2., k), p.2 * pow(2., k)), polygonConvexHullCartesian(points::MultiPoint)[1])])
FROM (SELECT arrayMap(i -> (toFloat64(cityHash64(number, i, 1) % 10000) / 100. - 50., toFloat64(cityHash64(number, i, 2) % 10000) / 100. - 50.), range(3 + number % 20)) AS points FROM numbers(100))
ARRAY JOIN [-1000, 1000] AS k
GROUP BY k
ORDER BY k;
SELECT polygonMinimumRotatedRectangleCartesian([(0., 0.), (4e-300, 0.), (3e-300, 1e-300)]::MultiPoint);

-- A small coordinate next to a huge one is kept as it is.
SELECT polygonConvexHullCartesian([(1e300, 0.), (0., 1e300), (-1e-20, 0.)]::MultiPoint);
SELECT polygonConvexHullCartesian([(0., 0.), (1e200, 0.), (0., 1e-200)]::MultiPoint), polygonConvexHullCartesian([(0., 0.), (1e300, 0.), (0., 1e-300)]::MultiPoint);
-- A huge side and a tiny side. The three rectangles of a right triangle have the same area, so only the area and the height are checked.
SELECT k, round(polygonAreaCartesian(r), 6), arrayMin(arrayMap(p -> p.2, r[1])) <= 0 AND arrayMax(arrayMap(p -> p.2, r[1])) >= pow(10., -k)
FROM (SELECT k, polygonMinimumRotatedRectangleCartesian([(0., 0.), (pow(10., k), 0.), (0., pow(10., -k))]::MultiPoint) AS r FROM (SELECT arrayJoin([200, 250, 300]) AS k))
ORDER BY k;
SELECT polygonMinimumRotatedRectangleCartesian([(0., 0.), (1e300, 0.), (1e300, 1e-300), (0., 1e-300)]::MultiPoint);
SELECT polygonMinimumRotatedRectangleCartesian([(0., 0.), (1e-250, 0.), (1e-250, 1e250), (0., 1e250)]::MultiPoint);
-- A rectangle with sides of 1e300 and 1e-300 (in both orientations) keeps its sides.
-- The coordinates are divided by the scales of the axes and rounded, because a corner of a rotated rectangle can be off by one ulp of 1e300.
SELECT arrayMap(p -> (round(p.1 / scale.1, 9), round(p.2 / scale.2, 9)), polygonMinimumRotatedRectangleCartesian(points::MultiPoint)[1])
FROM (SELECT arrayJoin([
    ([(0., 0.), (1e300, 0.), (1e300, 1e-300), (0., 1e-300)], (1e300, 1e-300)),
    ([(0., 0.), (1e-300, 0.), (1e-300, 1e300), (0., 1e300)], (1e-300, 1e300)),
    ([(0., 0.), (1e250, 0.), (1e250, 1e-250), (0., 1e-250)], (1e250, 1e-250)),
    ([(0., 0.), (1e300, 1e300), (1e300, 1e-300)], (1e300, 1e300)),
    -- Huge coordinates, where the area is beyond the range of `Float64`.
    ([(0., 0.), (4e300, 3e300), (1e300, 2e300)], (1e300, 1e300)),
    -- A triangle with a diagonal edge, whose tiny x difference must not round to zero next to its huge y difference.
    ([(0., 0.), (1e-250, 0.), (1e-250, 1e250)], (1e-250, 1e250)),
    ([(0., 0.), (pow(2., -700), 0.), (pow(2., -700), pow(2., 700))], (pow(2., -700), pow(2., 700)))]) AS t, t.1 AS points, t.2 AS scale);
-- Tiny points next to a far one, where the products of their differences would underflow: (3e-201, 5e-201) is inside.
SELECT polygonConvexHullCartesian([(1., 1.), (-9e-201, 2e-201), (-3e-201, -4e-201), (3e-201, 5e-201)]::MultiPoint);
-- Differences of coordinates near the largest `Float64` overflow by themselves.
SELECT polygonConvexHullCartesian([(1.5e308, 1e-200), (-1.5e308, -1e-180), (1e200, 0.)]::MultiPoint);
-- An axis from 2e-310 to 1.5e308 spans more than `Float64` can hold after scaling: the subnormal coordinate rounds, but nothing overflows.
SELECT length(h[1]), arrayAll(p -> isFinite(p.1) AND isFinite(p.2), h[1]), arrayAll(p -> isFinite(p.1) AND isFinite(p.2), r[1])
FROM (SELECT polygonConvexHullCartesian(points) AS h, polygonMinimumRotatedRectangleCartesian(points) AS r
    FROM (SELECT [(0., 2e-310), (1., 1.5e308), (2., -1.5e308)]::MultiPoint AS points));

-- Edges so short that their products underflow still have a direction.
SELECT polygonMinimumRotatedRectangleCartesian([(0., 0.), (1., 0.), (1e-170, 1e-170)]::MultiPoint);
SELECT polygonMinimumRotatedRectangleCartesian([(0., 1.), (0., 0.), (1e-170, 0.), (1., 1.)]::MultiPoint);
SELECT polygonMinimumRotatedRectangleCartesian([(0., 0.), (1., 0.), (1., 1e-170), (0., 1e-170)]::MultiPoint);

-- A nearly flat triangle: the rectangle goes from (-0.077, -0.493) to (0.129, 0.834).
SELECT polygonMinimumRotatedRectangleCartesian([(0.06571220733223056, 0.42349718420849747), (-0.07657354672902189, -0.4934955427785932), (0.12942912117569202, 0.8341352481946127)]::MultiPoint);

-- A short edge next to long ones does not stop the calipers, even if it changes nothing in the rounded distances.
SELECT polygonAreaCartesian(polygonMinimumRotatedRectangleCartesian([(-7.679e18, 100559.99), (4.17e-16, 3.109e16), (3.8165e21, -0.45212), (-8.11e-10, 2.685e-19), (-0.006049, 4.88e-19)]::MultiPoint)) > 1e35;

-- A square rotated by 45 degrees: the bounding box has twice its area.
SELECT polygonMinimumRotatedRectangleCartesian([(0., 1.), (1., 0.), (2., 1.), (1., 2.)]::Ring) AS r, polygonAreaCartesian(r);

-- The other geometry types.
SELECT polygonMinimumRotatedRectangleCartesian([[[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]], [[(3., 0.), (4., 0.), (4., 1.), (3., 0.)]]]::MultiPolygon);
SELECT polygonMinimumRotatedRectangleCartesian([[(0., 0.), (0., 3.)], [(1., 0.), (1., 3.)]]::MultiLineString);
SELECT polygonMinimumRotatedRectangleCartesian(readWKT('LINESTRING (0 0, 4 0, 3 1)'));

-- A single point, collinear points and no points.
SELECT polygonMinimumRotatedRectangleCartesian([(5., 5.), (5., 5.)]::MultiPoint);
SELECT polygonMinimumRotatedRectangleCartesian([(0., 0.), (1., 1.), (3., 3.)]::LineString);
SELECT polygonMinimumRotatedRectangleCartesian([]::Ring);

-- As for `polygonConvexHullCartesian`, a point is not accepted.
SELECT polygonMinimumRotatedRectangleCartesian((0., 0.)); -- { serverError BAD_ARGUMENTS }

-- The rectangle has the smallest area among the rectangles on the edges of the convex hull,
-- its sides are perpendicular, and it contains all points.
WITH
    arrayMap(i -> (toFloat64(cityHash64(number, i, 1) % 10000) / 100., toFloat64(cityHash64(number, i, 2) % 10000) / 100.), range(3 + number % 40)) AS raw,
    arrayMap(p -> (p.1 + 0.5 * p.2, p.2 - 0.3 * p.1), raw) AS points,
    polygonConvexHullCartesian(points::MultiPoint)[1] AS hull,
    arrayFilter(e -> e != (0., 0.), arrayMap(i -> (hull[i + 1].1 - hull[i].1, hull[i + 1].2 - hull[i].2), range(1, length(hull)))) AS edges,
    arrayMin(arrayMap(e ->
        (arrayMax(arrayMap(p -> p.1 * e.1 + p.2 * e.2, hull)) - arrayMin(arrayMap(p -> p.1 * e.1 + p.2 * e.2, hull)))
        * (arrayMax(arrayMap(p -> p.2 * e.1 - p.1 * e.2, hull)) - arrayMin(arrayMap(p -> p.2 * e.1 - p.1 * e.2, hull)))
        / (e.1 * e.1 + e.2 * e.2), edges)) AS min_area,
    polygonMinimumRotatedRectangleCartesian(points::MultiPoint)[1] AS r,
    (r[2].1 - r[1].1, r[2].2 - r[1].2) AS side1,
    (r[4].1 - r[1].1, r[4].2 - r[1].2) AS side2,
    side1.1 * side1.1 + side1.2 * side1.2 AS square1,
    side2.1 * side2.1 + side2.2 * side2.2 AS square2
SELECT
    count(),
    countIf(length(r) = 5 AND r[5] = r[1] AND abs((r[3].1 - r[1].1) - (side1.1 + side2.1)) < 1e-9 AND abs((r[3].2 - r[1].2) - (side1.2 + side2.2)) < 1e-9),
    countIf(abs(side1.1 * side2.1 + side1.2 * side2.2) <= 1e-9 * (square1 + square2)),
    countIf(abs(sqrt(square1 * square2) - min_area) <= 1e-9 * min_area),
    countIf(arrayAll(p ->
        ((p.1 - r[1].1) * side1.1 + (p.2 - r[1].2) * side1.2) / square1 BETWEEN -1e-9 AND 1 + 1e-9
        AND ((p.1 - r[1].1) * side2.1 + (p.2 - r[1].2) * side2.2) / square2 BETWEEN -1e-9 AND 1 + 1e-9, points))
FROM numbers(300);

SELECT '-- exteriorRing, interiorRingN';

WITH [[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(2., 2.), (4., 2.), (4., 4.), (2., 2.)], [(6., 6.), (8., 6.), (8., 8.), (6., 6.)]]::Polygon AS p
SELECT exteriorRing(p), toTypeName(exteriorRing(p)), interiorRingN(p, 1), interiorRingN(p, 2), interiorRingN(p, -1), interiorRingN(p, -2), toTypeName(interiorRingN(p, 1));

-- There is no such ring.
WITH [[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(2., 2.), (4., 2.), (4., 4.), (2., 2.)]]::Polygon AS p
SELECT interiorRingN(p, 0), interiorRingN(p, 2), interiorRingN(p, -2), interiorRingN(p, 9223372036854775807), interiorRingN(p, -9223372036854775808), interiorRingN(p, 18446744073709551615);
SELECT exteriorRing([]::Polygon), interiorRingN([]::Polygon, 1), interiorRingN([[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]::Polygon, 1);

-- An array of arrays of points without a type name is a `Polygon`.
SELECT exteriorRing([[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]) AS r, toTypeName(r);

SELECT number, interiorRingN([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(2., 2.), (4., 2.), (4., 4.), (2., 2.)], [(6., 6.), (8., 6.), (8., 8.), (6., 6.)]]::Polygon, number - 2) FROM numbers(5);
SELECT exteriorRing(p), interiorRingN(p, toUInt8(1)) FROM (SELECT arrayJoin([[[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]], [], [[(5., 5.), (6., 5.), (6., 6.), (5., 5.)], [(5.1, 5.1), (5.2, 5.1), (5.2, 5.2), (5.1, 5.1)]]]::Array(Polygon)) AS p);

SELECT '-- pointN';

WITH [(1., 2.), (3., 4.), (5., 6.)]::LineString AS l
SELECT pointN(l, 1), pointN(l, 3), pointN(l, -1), pointN(l, -3), toTypeName(pointN(l, 1));

-- There is no such point.
WITH [(1., 2.), (3., 4.), (5., 6.)]::LineString AS l
SELECT pointN(l, 0), pointN(l, 4), pointN(l, -4), pointN(l, 9223372036854775807), pointN(l, -9223372036854775808), pointN(l, 18446744073709551615), pointN([]::LineString, 1);

-- An array of points without a type name is a `Ring`.
SELECT pointN([(0., 0.), (1., 0.), (1., 1.), (0., 0.)]::Ring, 2), pointN([(0., 0.), (1., 0.), (1., 1.), (0., 0.)], -2);

SELECT number, pointN([(1., 2.), (3., 4.), (5., 6.)]::LineString, toInt8(number) - 3) FROM numbers(7);
SELECT pointN(l, n) FROM (SELECT arrayJoin([[(1., 2.)], [], [(3., 4.), (5., 6.)]]::Array(LineString)) AS l, arrayJoin([1, 2]) AS n);

SELECT '-- Corners of the minimum rectangle';

SELECT arrayMap(n -> pointN(exteriorRing(polygonMinimumRotatedRectangleCartesian([[(0., 0.), (0., 1.), (2., 1.), (2., 0.), (0., 0.)]]::Polygon)), n), range(1, 5));

SELECT '-- Aliases';

SELECT ST_ExteriorRing([[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]::Polygon), ST_InteriorRingN([[(0., 0.), (9., 0.), (9., 9.), (0., 0.)], [(1., 1.), (2., 1.), (2., 2.), (1., 1.)]]::Polygon, 1), ST_PointN([(1., 2.), (3., 4.)]::LineString, 2);

SELECT '-- Geometry';

SELECT
    variantType(g),
    exteriorRing(g) AS ring,
    toTypeName(ring),
    variantType(ring),
    interiorRingN(g, 1),
    interiorRingN(g, 2),
    pointN(g, 2) AS point,
    variantType(point),
    pointN(g, -1),
    pointN(g, 3)
FROM
(
    SELECT arrayJoin([
        readWKT('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 4 2, 4 4, 2 2))'),
        readWKT('LINESTRING (1 2, 3 4)'),
        [(0., 0.), (1., 0.), (1., 1.), (0., 0.)]::Ring::Geometry,
        readWKT('POINT (1 2)'),
        readWKT('MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)))'),
        readWKT('MULTIPOINT (1 2, 3 4)'),
        NULL::Geometry]) AS g
);

SELECT number, pointN(readWKT('LINESTRING (1 2, 3 4, 5 6)'), number) FROM numbers(4);

SELECT '-- Errors';

SELECT exteriorRing([(0., 0.), (1., 1.)]::LineString); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT exteriorRing([[[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]]::MultiPolygon); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT interiorRingN([[(0., 0.), (1., 1.)]]::MultiLineString, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT pointN([[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]::Polygon, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT pointN([(0., 0.), (1., 1.)]::MultiPoint, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT pointN((0., 0.), 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT pointN([(0., 0.), (1., 1.)]::LineString, 1.5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT pointN([(0., 0.), (1., 1.)]::LineString, '1'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT pointN([(0., 0.), (1., 1.)]::LineString); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT exteriorRing([[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]::Polygon, 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
