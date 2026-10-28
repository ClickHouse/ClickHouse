-- Boundaries a few ULPs apart are separated mathematically, but the geometry validator treats
-- them as touching. Shortcuts that concatenate components must not produce such geometry,
-- because the resulting state would be rejected when it is deserialized.

SET max_threads = 1;

CREATE TEMPORARY TABLE holes (id UInt64, p Polygon);
INSERT INTO holes VALUES
    (1, [[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(1., 1.), (1., 3.), (3., 3.), (3., 1.), (1., 1.)]]),
    (2, [[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(3.0000000000000004, 1.), (3.0000000000000004, 3.), (5., 3.), (5., 1.), (3.0000000000000004, 1.)]]);

SELECT 'intersection';
SELECT wkt(groupPolygonIntersection(p)) FROM (SELECT p FROM holes ORDER BY id);
SELECT wkt(groupPolygonIntersection(p)) FROM (SELECT p FROM holes ORDER BY id DESC);

SELECT 'intersection state round trip';
SELECT wkt(finalizeAggregation(CAST(unhex(hex(s)) AS AggregateFunction(groupPolygonIntersection, Polygon))))
FROM (SELECT groupPolygonIntersectionState(p) AS s FROM (SELECT p FROM holes ORDER BY id));

SELECT 'intersection with separate components';
-- The first row has two separated holes, so the next nearly adjacent hole reaches the component set.
SELECT wkt(groupPolygonIntersection(p)) FROM
(
    SELECT [[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(1., 1.), (1., 3.), (3., 3.), (3., 1.), (1., 1.)], [(7., 1.), (7., 3.), (9., 3.), (9., 1.), (7., 1.)]]::Polygon AS p
    UNION ALL
    SELECT [[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(3.0000000000000004, 1.), (3.0000000000000004, 3.), (5., 3.), (5., 1.), (3.0000000000000004, 1.)]]::Polygon
);

SELECT 'union';
CREATE TEMPORARY TABLE rectangles (id UInt64, p Polygon);
INSERT INTO rectangles VALUES
    (1, [[(-4., 1.), (-4., 3.), (-2., 3.), (-2., 1.), (-4., 1.)]]),
    (2, [[(1., 1.), (1., 3.), (3., 3.), (3., 1.), (1., 1.)]]),
    (3, [[(3.0000000000000004, 1.), (3.0000000000000004, 3.), (5., 3.), (5., 1.), (3.0000000000000004, 1.)]]);
SELECT wkt(groupPolygonUnion(p)) FROM (SELECT p FROM rectangles ORDER BY id);
SELECT wkt(groupPolygonUnion(p)) FROM (SELECT p FROM rectangles ORDER BY id DESC);
SELECT wkt(finalizeAggregation(CAST(unhex(hex(s)) AS AggregateFunction(groupPolygonUnion, Polygon))))
FROM (SELECT groupPolygonUnionState(p) AS s FROM (SELECT p FROM rectangles ORDER BY id));
