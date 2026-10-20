-- An empty `MultiPolygon` or `MultiLineString` row must not make the next rows of the block empty.

SELECT number, wkt(p), polygonAreaCartesian(p), polygonPerimeterCartesian(p)
FROM
(
    SELECT number, if(number = 0, CAST([], 'MultiPolygon'), CAST([[[(0., 0.), (0., 1.), (1., 1.), (1., 0.), (0., 0.)]]], 'MultiPolygon')) AS p
    FROM numbers(3)
)
ORDER BY number;

SELECT number, wkt(p), polygonAreaCartesian(p)
FROM
(
    SELECT number, if(number < 3, CAST([], 'MultiPolygon'), CAST([[[(0., 0.), (0., 2.), (2., 2.), (2., 0.), (0., 0.)]], [[(5., 5.), (5., 6.), (6., 6.), (6., 5.), (5., 5.)]]], 'MultiPolygon')) AS p
    FROM numbers(5)
)
ORDER BY number;

SELECT number, wkt(l)
FROM
(
    SELECT number, if(number = 0, CAST([], 'MultiLineString'), CAST([[(0., 0.), (1., 1.)]], 'MultiLineString')) AS l
    FROM numbers(3)
)
ORDER BY number;
