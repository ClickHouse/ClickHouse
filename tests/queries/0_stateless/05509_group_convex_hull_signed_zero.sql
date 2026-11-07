-- `+0.0` and `-0.0` compare equal, so the hull must not depend on which of them comes first.

CREATE TEMPORARY TABLE points (id UInt64, p Point);
INSERT INTO points VALUES (1, (0., 0.)), (2, (-0., -0.)), (3, (1., 0.)), (4, (0., 1.));

SELECT 'result';
SELECT arrayMap(q -> (1 / q.1, 1 / q.2), groupConvexHull(p)) FROM (SELECT p FROM points ORDER BY id) SETTINGS max_threads = 1;
SELECT arrayMap(q -> (1 / q.1, 1 / q.2), groupConvexHull(p)) FROM (SELECT p FROM points ORDER BY id DESC) SETTINGS max_threads = 1;

SELECT 'state';
SELECT hex(groupConvexHullState(p)) = (SELECT hex(groupConvexHullState(p)) FROM (SELECT p FROM points ORDER BY id DESC) SETTINGS max_threads = 1)
FROM (SELECT p FROM points ORDER BY id) SETTINGS max_threads = 1;

SELECT 'deserialized negative zero';
-- A hand-written version 3 state with the single point (-0, -0).
SELECT arrayMap(q -> (1 / q.1, 1 / q.2), finalizeAggregation(CAST(unhex('030101' || '0000000000000080' || '0000000000000080') AS AggregateFunction(groupConvexHull, Point))));
