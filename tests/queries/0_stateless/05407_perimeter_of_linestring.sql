-- The perimeter of a `LineString` or a `MultiLineString` is its length.

SELECT perimeterCartesian([(0., 0.), (3., 4.)]::LineString);
SELECT perimeterCartesian([(0., 0.), (3., 4.), (3., 10.)]::LineString);
SELECT perimeterCartesian([[(0., 0.), (3., 4.)], [(0., 0.), (0., 2.)]]::MultiLineString);
SELECT perimeterCartesian([(0., 0.), (3., 4.)]::LineString::Geometry);
SELECT perimeterCartesian([[(0., 0.), (3., 4.)], [(0., 0.), (0., 2.)]]::MultiLineString::Geometry);

-- A closed line has the same length as the perimeter of the ring with the same points.
SELECT perimeterCartesian([(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]::LineString) = perimeterCartesian([(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]::Ring);
SELECT round(perimeterSpherical([(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]::LineString), 9) = round(perimeterSpherical([(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)]::Ring), 9);

SELECT round(perimeterSpherical([(0., 0.), (3., 4.)]::LineString), 9);
SELECT round(perimeterSpherical([[(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)], [(2., 2.), (3., 2.), (3., 3.), (2., 3.), (2., 2.)]]::MultiLineString), 9);
