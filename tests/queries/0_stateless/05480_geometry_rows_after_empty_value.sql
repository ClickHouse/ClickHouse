-- Tags: no-fasttest
-- no-fasttest: h3PolygonToCells needs the h3 library

-- Empty geometries at the start, in the middle and twice in a row must not change the result of the other rows of the block.

SELECT 'MultiPolygon';
SELECT n, wkt(mp), polygonAreaCartesian(mp), polygonPerimeterCartesian(mp), polygonAreaSpherical(mp) > 0, length(h3PolygonToCells(mp, 5)) > 0,
    polygonsWithinCartesian(mp, [[[(-1., -1.), (-1., 10.), (10., 10.), (10., -1.), (-1., -1.)]]]),
    wkt(polygonsIntersectionCartesian(mp, [[[(0., 0.), (0., 1.), (1., 1.), (1., 0.), (0., 0.)]]])),
    wkt(CAST(mp, 'Geometry'))
FROM values('n UInt8, mp MultiPolygon',
    (1, []),
    (2, [[[(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]]]),
    (3, []),
    (4, []),
    (5, [[[(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]]]),
    (6, [[[(0, 0), (0, 3), (3, 3), (3, 0), (0, 0)]], [[(5, 5), (5, 6), (6, 6), (6, 5), (5, 5)]]]))
ORDER BY n;

SELECT svg(mp) FROM values('n UInt8, mp MultiPolygon', (1, []), (2, [[[(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]]])) ORDER BY n;

SELECT 'MultiPolygon with empty polygons';
SELECT n, wkt(mp), polygonAreaCartesian(mp)
FROM values('n UInt8, mp MultiPolygon',
    (1, [[]]),
    (2, [[[(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]]]),
    (3, [[], [[(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]]]),
    (4, []),
    (5, [[[(0, 0), (0, 3), (3, 3), (3, 0), (0, 0)]]]))
ORDER BY n;

SELECT 'Nested';
SELECT groupArray(polygonAreaCartesian(mp)), groupArray(polygonAreaCartesian(polygonsIntersectionCartesian(mp, mp)))
FROM (SELECT arrayJoin([[], [], [[[(0., 0.), (0., 3.), (3., 3.), (3., 0.), (0., 0.)]]], [[[(0., 0.), (0., 3.), (3., 3.), (3., 0.), (0., 0.)]]], [[[(0., 0.), (0., 3.), (3., 3.), (3., 0.), (0., 0.)]]], [[[(0., 0.), (0., 3.), (3., 3.), (3., 0.), (0., 0.)]]]])::MultiPolygon AS mp);

SELECT 'MultiLineString';
SELECT n, wkt(ml), wkt(CAST(ml, 'Geometry'))
FROM values('n UInt8, ml MultiLineString',
    (1, []),
    (2, [[(0, 0), (0, 1)]]),
    (3, []),
    (4, []),
    (5, [[(0, 0), (0, 2)]]),
    (6, [[(0, 0), (0, 3)], [(1, 1), (2, 2)]]))
ORDER BY n;

SELECT 'Polygon';
SELECT n, wkt(p), polygonAreaCartesian(p), polygonPerimeterCartesian(p), wkt(CAST(p, 'Geometry'))
FROM values('n UInt8, p Polygon',
    (1, []),
    (2, [[(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]]),
    (3, []),
    (4, []),
    (5, [[(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]]))
ORDER BY n;

SELECT 'Geometry';
SELECT n, wkt(g)
FROM
(
    SELECT n, if(n = 3, CAST(p, 'Geometry'), CAST(mp, 'Geometry')) AS g
    FROM values('n UInt8, p Polygon, mp MultiPolygon',
        (1, [], []),
        (2, [], [[[(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]]]),
        (3, [[(0, 0), (0, 4), (4, 4), (4, 0), (0, 0)]], []),
        (4, [], []),
        (5, [], [[[(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]]]),
        (6, [], [[[(0, 0), (0, 3), (3, 3), (3, 0), (0, 0)]]]))
)
ORDER BY n;
