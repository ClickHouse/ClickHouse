-- Tags: no-fasttest
-- Tag no-fasttest: needs s2

SELECT 'centroidSpherical Point';
SELECT centroidSpherical((2., 3.));

SELECT 'centroidSpherical Ring';
SELECT round(centroidSpherical([(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]).1, 6), round(centroidSpherical([(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]).2, 6);

SELECT 'centroidSpherical Polygon';
SELECT round(centroidSpherical([[(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]]).1, 6), round(centroidSpherical([[(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]]).2, 6);

SELECT 'centroidSpherical LineString';
SELECT round(centroidSpherical(CAST([(0., 0.), (10., 0.)], 'LineString')).1, 6), round(centroidSpherical(CAST([(0., 0.), (10., 0.)], 'LineString')).2, 6);

SELECT 'centroidSpherical MultiLineString';
SELECT round(centroidSpherical(CAST([[(0., 0.), (5., 0.)], [(5., 0.), (10., 0.)]], 'MultiLineString')).1, 6), round(centroidSpherical(CAST([[(0., 0.), (5., 0.)], [(5., 0.), (10., 0.)]], 'MultiLineString')).2, 6);

SELECT 'centroidSpherical MultiPolygon';
SELECT round(centroidSpherical([[[(0., 0.), (0., 2.), (2., 2.), (2., 0.), (0., 0.)]], [[(4., 4.), (4., 6.), (6., 6.), (6., 4.), (4., 4.)]]]).1, 6), round(centroidSpherical([[[(0., 0.), (0., 2.), (2., 2.), (2., 0.), (0., 0.)]], [[(4., 4.), (4., 6.), (6., 6.), (6., 4.), (4., 4.)]]]).2, 6);

SELECT 'centroidSpherical date line crossing LineString';
SELECT round(centroidSpherical(CAST([(179., 0.), (-179., 0.)], 'LineString')).1, 6), round(centroidSpherical(CAST([(179., 0.), (-179., 0.)], 'LineString')).2, 6);

SELECT 'centroidSpherical near North Pole';
SELECT round(centroidSpherical([(0., 89.), (120., 89.), (240., 89.), (0., 89.)]).1, 6), round(centroidSpherical([(0., 89.), (120., 89.), (240., 89.), (0., 89.)]).2, 6);

SELECT 'centroidSpherical Polygon with hole';
SELECT round(centroidSpherical([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(3., 3.), (3., 7.), (7., 7.), (7., 3.), (3., 3.)]]).1, 6), round(centroidSpherical([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(3., 3.), (3., 7.), (7., 7.), (7., 3.), (3., 3.)]]).2, 6);

SELECT 'centroidSpherical square';
SELECT round(centroidSpherical([(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)]).1, 6), round(centroidSpherical([(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)]).2, 6);

SELECT 'centroidSpherical Geometry Point';
SELECT centroidSpherical(readWKT('POINT(2 3)'));
SELECT 'centroidSpherical Geometry Polygon';
SELECT round(centroidSpherical(readWKT('POLYGON((0 0, 0 4, 4 4, 4 0, 0 0))')).1, 6), round(centroidSpherical(readWKT('POLYGON((0 0, 0 4, 4 4, 4 0, 0 0))')).2, 6);

SELECT 'centroidSpherical globe-spanning polygon (triggers adaptive origin)';
SELECT round(centroidSpherical([(0., 0.), (90., 0.), (180., 0.), (-90., 0.), (0., 0.)]).1, 6), round(centroidSpherical([(0., 0.), (90., 0.), (180., 0.), (-90., 0.), (0., 0.)]).2, 6);

SELECT centroidSpherical(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'centroidSpherical empty polygon (degenerate)';
SELECT round(centroidSpherical([[(0., 0.), (0., 0.), (0., 0.), (0., 0.)]]).1, 6), round(centroidSpherical([[(0., 0.), (0., 0.), (0., 0.), (0., 0.)]]).2, 6);

SELECT 'centroidSpherical MultiPoint';
SELECT round(centroidSpherical(CAST([(0., 0.), (3., 5.), (6., 0.)], 'MultiPoint')).1, 6), round(centroidSpherical(CAST([(0., 0.), (3., 5.), (6., 0.)], 'MultiPoint')).2, 6);

SELECT 'centroidSpherical MultiPoint along meridian';
SELECT round(centroidSpherical(CAST([(0., 10.), (0., 20.), (0., 30.)], 'MultiPoint')).1, 6), round(centroidSpherical(CAST([(0., 10.), (0., 20.), (0., 30.)], 'MultiPoint')).2, 6);

SELECT 'centroidSpherical empty MultiPoint (degenerate)';
SELECT round(centroidSpherical(CAST([], 'MultiPoint')).1, 6), round(centroidSpherical(CAST([], 'MultiPoint')).2, 6);

SELECT 'centroidSpherical Polygon with off-center hole, hole wound like the outer ring and reversed, and unclosed';
SELECT round(c.1, 6), round(c.2, 6) FROM (SELECT centroidSpherical([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(1., 1.), (1., 3.), (3., 3.), (3., 1.), (1., 1.)]]) AS c);
SELECT round(c.1, 6), round(c.2, 6) FROM (SELECT centroidSpherical([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(1., 1.), (3., 1.), (3., 3.), (1., 3.), (1., 1.)]]) AS c);
SELECT round(c.1, 6), round(c.2, 6) FROM (SELECT centroidSpherical([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(1., 1.), (3., 1.), (3., 3.), (1., 3.)]]) AS c);

SELECT 'centroidSpherical MultiPolygon with mixed winding';
SELECT round(c.1, 6), round(c.2, 6) FROM (SELECT centroidSpherical([[[(0., 0.), (0., 2.), (2., 2.), (2., 0.), (0., 0.)]], [[(4., 4.), (6., 4.), (6., 6.), (4., 6.), (4., 4.)]]]) AS c);

-- A block with more rows than member geometries: an empty row must not shift or drop the following rows.
SELECT 'centroidSpherical MultiLineString empty row followed by nonempty rows';
SELECT g, round(c.1, 6), round(c.2, 6)
FROM (SELECT g, centroidSpherical(g) AS c FROM (SELECT CAST(arrayJoin([[], [[(10., 10.), (12., 12.)]]]), 'MultiLineString') AS g));

SELECT 'centroidSpherical MultiPolygon empty row followed by nonempty rows';
SELECT g, round(c.1, 6), round(c.2, 6)
FROM (SELECT g, centroidSpherical(g) AS c FROM (SELECT CAST(arrayJoin([[], [[[(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)]]]]), 'MultiPolygon') AS g));
