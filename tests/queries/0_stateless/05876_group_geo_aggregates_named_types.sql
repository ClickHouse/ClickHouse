-- Named geo types that share the storage layout of areal types must not be reinterpreted as polygons.
-- `LineString` and `MultiPoint` are stored as `Ring`, `MultiLineString` as `Polygon`.

SELECT groupPolygonUnion(x) FROM (SELECT [(0., 0.), (1., 0.), (1., 1.), (0., 0.)]::LineString AS x); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonUnion(x) FROM (SELECT [(0., 0.), (1., 0.), (1., 1.), (0., 0.)]::MultiPoint AS x); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonUnion(x) FROM (SELECT [[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]::MultiLineString AS x); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonIntersection(x) FROM (SELECT [(0., 0.), (1., 0.), (1., 1.), (0., 0.)]::LineString AS x); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonIntersection(x) FROM (SELECT [(0., 0.), (1., 0.), (1., 1.), (0., 0.)]::MultiPoint AS x); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonIntersection(x) FROM (SELECT [[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]::MultiLineString AS x); -- { serverError BAD_ARGUMENTS }

-- A type without a custom name is resolved by its structure, as for the other geo functions.
SELECT 'union unnamed', wkt(groupPolygonUnion(x)) FROM (SELECT [(0., 0.), (2., 0.), (2., 2.), (0., 2.), (0., 0.)] AS x);
SELECT 'intersection unnamed', wkt(groupPolygonIntersection(x)) FROM (SELECT [[(0., 0.), (2., 0.), (2., 2.), (0., 2.), (0., 0.)]] AS x);

-- `MultiPoint` input of `groupConvexHull`.
SELECT 'hull MultiPoint', wkt(groupConvexHull(mp)) FROM
(
    SELECT arrayJoin([[(0., 0.), (4., 0.), (2., 1.)], [(4., 4.), (0., 4.), (2., 2.)]])::MultiPoint AS mp
);

-- `MultiPoint` inside `Geometry`, alone and mixed with other geometries.
SELECT 'hull Geometry MultiPoint', wkt(groupConvexHull(g)) FROM
(
    SELECT [(0., 0.), (4., 0.), (4., 4.), (0., 4.), (2., 2.)]::MultiPoint::Geometry AS g
);
SELECT 'hull Geometry mixed', wkt(groupConvexHull(g)) FROM
(
    SELECT [(0., 0.), (4., 0.)]::MultiPoint::Geometry AS g
    UNION ALL SELECT (2., 6.)::Point::Geometry
    UNION ALL SELECT [[(-1., 1.), (-1., 2.)]]::MultiLineString::Geometry
    UNION ALL SELECT NULL::Geometry
);

-- Every line of a `MultiLineString` contributes to the hull.
SELECT 'hull MultiLineString', wkt(groupConvexHull(mls)) FROM
(
    SELECT [[(0., 0.), (1., 0.)], [(5., 5.), (5., 6.)]]::MultiLineString AS mls
);

-- Constant `Geometry` argument.
SELECT 'hull const Geometry', wkt(groupConvexHull([(0., 0.), (3., 0.), (0., 3.)]::MultiPoint::Geometry)) FROM numbers(3);

-- A malformed serialized state is rejected with a proper exception.
SELECT CAST(unhex('01ffffffffffffffff01'), 'AggregateFunction(groupConvexHull, Point)'); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT CAST(concat(unhex('0105'), 'hello'), 'AggregateFunction(groupConvexHull, Point)'); -- { serverError INCORRECT_DATA }
SELECT CAST(concat(unhex('0105'), 'hello'), 'AggregateFunction(groupPolygonUnion, Polygon)'); -- { serverError INCORRECT_DATA }
SELECT CAST(concat(unhex('010a'), 'POINT(1 2)'), 'AggregateFunction(groupPolygonIntersection, Polygon)'); -- { serverError INCORRECT_DATA }

-- A serialized state round-trips.
SELECT 'state roundtrip', wkt(finalizeAggregation(CAST(unhex(hex(groupConvexHullState(p))), 'AggregateFunction(groupConvexHull, Point)')))
FROM (SELECT arrayJoin([(0., 0.), (1., 0.), (0., 1.)])::Point AS p);
