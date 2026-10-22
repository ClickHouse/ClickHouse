-- A polygon with an empty exterior ring and a non-empty interior ring led to reading uninitialized memory.
SELECT polygonConvexHullCartesian([[[], [(0., 0.), (1., 1.), (2., 0.)]]]::MultiPolygon);
SELECT polygonConvexHullCartesian([[], [(0., 0.), (1., 1.), (2., 0.)]]::Polygon);
SELECT polygonConvexHullCartesian([[[], [(0., 0.), (1., 1.)]], [[], [(5., 5.), (6., 6.)]]]::MultiPolygon);
SELECT wkt(polygonConvexHullCartesian([[[], [(0., 0.), (1., 1.)]], [[(0., 0.), (0., 5.), (5., 5.), (5., 0.), (2., 3.)]]]::MultiPolygon));
SELECT polygonConvexHullCartesian(arrayJoin([[[[], [(0., 0.), (1., 1.)]]], [[[(0., 0.), (0., 5.), (5., 5.)]]]])::MultiPolygon);
