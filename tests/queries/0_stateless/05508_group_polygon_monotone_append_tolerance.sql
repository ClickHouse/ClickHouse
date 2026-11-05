-- A monotone hole that starts a few ULPs right of a step of the previous hole is separated
-- mathematically, but the geometry validator treats the junction as a spike. The monotone append
-- shortcut must not produce such a hole, because the state would be rejected when it is deserialized.

SET max_threads = 1;

CREATE TEMPORARY TABLE stepped (id UInt64, p Polygon);
INSERT INTO stepped VALUES
    (1, [[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(1., 1.), (2., 1.), (2., 2.), (3., 2.), (3., 4.), (1., 4.), (1., 1.)]]),
    (2, [[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(2.0000000000000004, 1.), (4., 1.), (4., 5.), (2.0000000000000004, 5.), (2.0000000000000004, 1.)]]);

SELECT 'result';
SELECT wkt(groupPolygonIntersection(p)) FROM (SELECT p FROM stepped ORDER BY id);
SELECT wkt(groupPolygonIntersection(p)) FROM (SELECT p FROM stepped ORDER BY id DESC);

SELECT 'state round trip';
SELECT wkt(finalizeAggregation(CAST(unhex(hex(s)) AS AggregateFunction(groupPolygonIntersection, Polygon))))
FROM (SELECT groupPolygonIntersectionState(p) AS s FROM (SELECT p FROM stepped ORDER BY id));
SELECT wkt(finalizeAggregation(CAST(unhex(hex(s)) AS AggregateFunction(groupPolygonIntersection, Polygon))))
FROM (SELECT groupPolygonIntersectionState(p) AS s FROM (SELECT p FROM stepped ORDER BY id DESC));

SELECT 'clearly separated steps still join';
SELECT wkt(groupPolygonIntersection(p)) FROM
(
    SELECT p FROM
    (
        SELECT 1 AS id, [[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(1., 1.), (3., 1.), (3., 4.), (1., 4.), (1., 1.)]]::Polygon AS p
        UNION ALL
        SELECT 2 AS id, [[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(2., 2.), (5., 2.), (5., 5.), (2., 5.), (2., 2.)]]::Polygon AS p
    )
    ORDER BY id
);
