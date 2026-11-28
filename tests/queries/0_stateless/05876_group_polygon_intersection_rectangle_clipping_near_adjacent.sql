-- Rectangle clipping intersects the components of two rectangle sets pairwise. Each piece is a valid
-- polygon, but the pieces of nearly adjacent input components can be within the tolerance of the
-- geometry validator, which rejects them as a multi-polygon. Such inputs must use the general overlay
-- and give the same result as `polygonsIntersectionCartesian`.

SET max_threads = 1;

WITH
    readWKTMultiPolygon('MULTIPOLYGON(((3 2.0000000000000004,3 6.0000000000000027,5.9999999999999973 6.0000000000000027,5.9999999999999973 2.0000000000000004,3 2.0000000000000004)),((1.0000000000000004 2,1.0000000000000004 4.0000000000000036,2.9999999999999996 4.0000000000000036,2.9999999999999996 2,1.0000000000000004 2)))') AS a,
    readWKTMultiPolygon('MULTIPOLYGON(((2 3,2 3.000000000000001,4 3.000000000000001,4 3,2 3)))') AS b
SELECT
    'near_adjacent',
    polygonAreaCartesian(groupPolygonIntersection(p)) = polygonAreaCartesian(polygonsIntersectionCartesian(a, b)),
    polygonAreaCartesian(groupPolygonIntersection(p)) > 0
FROM (SELECT arrayJoin([a, b]) AS p);

-- The same through partial states.
WITH
    readWKTMultiPolygon('MULTIPOLYGON(((3 2.0000000000000004,3 6.0000000000000027,5.9999999999999973 6.0000000000000027,5.9999999999999973 2.0000000000000004,3 2.0000000000000004)),((1.0000000000000004 2,1.0000000000000004 4.0000000000000036,2.9999999999999996 4.0000000000000036,2.9999999999999996 2,1.0000000000000004 2)))') AS a,
    readWKTMultiPolygon('MULTIPOLYGON(((2 3,2 3.000000000000001,4 3.000000000000001,4 3,2 3)))') AS b
SELECT
    'near_adjacent_merge',
    polygonAreaCartesian(groupPolygonIntersectionMerge(s)) = polygonAreaCartesian(polygonsIntersectionCartesian(a, b))
FROM (SELECT groupPolygonIntersectionState(p) AS s FROM (SELECT arrayJoin([a, b]) AS p, rowNumberInAllBlocks() AS n) GROUP BY n);

-- Pieces that are far apart or touch only at corners keep the exact rectangles.
SELECT 'separated', length(g), polygonAreaCartesian(g), arrayAll(p -> length(p) = 1 AND length(p[1]) = 5, g)
FROM
(
    SELECT groupPolygonIntersection(mp) AS g FROM
    (
        SELECT arrayJoin([
            [[[(0.,0.),(0.,2.),(2.,2.),(2.,0.),(0.,0.)]], [[(5.,0.),(5.,2.),(7.,2.),(7.,0.),(5.,0.)]]],
            [[[(1.,1.),(1.,3.),(6.,3.),(6.,1.),(1.,1.)]], [[(10.,10.),(10.,11.),(11.,11.),(11.,10.),(10.,10.)]]]])::MultiPolygon AS mp
    )
);

SELECT 'corner_contact', length(g), polygonAreaCartesian(g)
FROM
(
    SELECT groupPolygonIntersection(mp) AS g FROM
    (
        SELECT arrayJoin([
            [[[(0.,0.),(0.,2.),(2.,2.),(2.,0.),(0.,0.)]], [[(2.,2.),(2.,4.),(4.,4.),(4.,2.),(2.,2.)]]],
            [[[(1.,1.),(1.,3.),(3.,3.),(3.,1.),(1.,1.)]], [[(10.,10.),(10.,11.),(11.,11.),(11.,10.),(10.,10.)]]]])::MultiPolygon AS mp
    )
);
