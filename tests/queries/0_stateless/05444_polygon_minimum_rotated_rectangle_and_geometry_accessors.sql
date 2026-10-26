SELECT '-- polygonMinimumRotatedRectangleCartesian';

-- A rectangle that is not rotated is its own minimum rectangle; interior points and holes don't matter.
SELECT polygonMinimumRotatedRectangleCartesian([[(0., 0.), (0., 1.), (2., 1.), (2., 0.), (0., 0.)], [(0.5, 0.2), (1., 0.2), (1., 0.8), (0.5, 0.2)]]::Polygon);
SELECT polygonMinimumRotatedRectangleCartesian([(0., 0.), (2., 1.), (1., 0.5), (0., 1.), (2., 0.)]::MultiPoint);

-- A rectangle of 5 by 2.5 rotated by atan(3/4), with points inside it. The corners are exact.
SELECT polygonMinimumRotatedRectangleCartesian([(0., 0.), (4., 3.), (2.5, 5.), (-1.5, 2.), (1., 1.5), (2., 3.)]::MultiPoint) AS r, polygonAreaCartesian(r);
-- Without its corner (2.5, 5), the rectangle is the same, but the convex hull is not.
SELECT polygonMinimumRotatedRectangleCartesian(points), polygonConvexHullCartesian(points)
FROM (SELECT [(0., 0.), (4., 3.), (2., 3.), (-1.5, 2.), (1., 1.5)]::MultiPoint AS points);

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
