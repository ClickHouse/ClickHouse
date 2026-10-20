-- The convex hull ignores interior rings, so a polygon whose exterior ring is empty has an empty hull.

SELECT polygonConvexHullCartesian([[], [(1., 1.), (2., 2.), (3., 1.)]]::Polygon);
SELECT polygonConvexHullCartesian([[], [(1., 1.)], [(5., 5.), (6., 6.)]]::Polygon);
SELECT polygonConvexHullCartesian([[[], [(10.0001, 10.0001), (0., 5.), (5., 5.), (0.0001, 9007199254740991.), (2., 3.)]]]);
SELECT polygonConvexHullCartesian([[[], [(1., 1.), (2., 2.), (3., 1.)]], [[], [(5., 5.), (6., 6.), (7., 5.)]]]::MultiPolygon);
SELECT polygonConvexHullCartesian([[], [(1., 1.), (2., 2.), (3., 1.)]]::Polygon::Geometry);
SELECT polygonConvexHullCartesian(CAST(arrayJoin(['[[(0., 0.), (0., 5.), (5., 5.), (5., 0.)]]', '[[], [(1., 1.), (2., 2.), (3., 1.)]]', '[[(0., 0.), (1., 1.), (2., 0.)]]', '[[], [(1., 1.), (2., 2.), (3., 1.)]]']), 'Polygon'));

-- Polygons with a non-empty exterior ring still define the hull.
SELECT polygonConvexHullCartesian([[[], [(10., 10.), (20., 20.), (30., 10.)]], [[(0., 0.), (0., 5.), (5., 5.), (5., 0.)]]]::MultiPolygon);
SELECT polygonConvexHullCartesian([[[(0., 0.), (0., 5.), (5., 5.), (5., 0.)]], [[], [(10., 10.), (20., 20.), (30., 10.)]]]::MultiPolygon);
SELECT polygonConvexHullCartesian([[], [(1., 1.), (2., 2.), (3., 1.)]]::MultiLineString);
