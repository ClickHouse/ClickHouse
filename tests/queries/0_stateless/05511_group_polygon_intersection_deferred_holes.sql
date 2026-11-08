-- Rows with a common exterior intersect to that exterior minus the union of their holes. Once the
-- state is large, overlapping non-rectangular holes are accumulated as a union and the difference
-- is computed once. Check the area against an independent `groupPolygonUnion` of the holes.

SET max_threads = 1;

CREATE TEMPORARY TABLE deferred_octagons (id UInt64, hole Ring, p Polygon);
INSERT INTO deferred_octagons
WITH arrayMap(k -> (100 + number * 0.5 + 10 * cos(k * pi() / 4), 500 + 10 * sin(k * pi() / 4)), range(8)) AS h,
    arrayPushBack(h, h[1]) AS ring
SELECT number, ring, [[(0., 0.), (0., 1000.), (1000., 1000.), (1000., 0.), (0., 0.)], ring] FROM numbers(300);

-- Expected: the exterior area minus the area of the union of all holes.
CREATE TEMPORARY TABLE expected AS
SELECT 1000000 - polygonAreaCartesian(groupPolygonUnion([hole]::Polygon)) AS area FROM deferred_octagons;

SELECT 'direct';
SELECT abs(polygonAreaCartesian(groupPolygonIntersection(p)) - (SELECT area FROM expected)) < 1e-6
FROM (SELECT p FROM deferred_octagons ORDER BY id);

SELECT 'shuffled';
SELECT abs(polygonAreaCartesian(groupPolygonIntersection(p)) - (SELECT area FROM expected)) < 1e-6
FROM (SELECT p FROM deferred_octagons ORDER BY cityHash64(id));

SELECT 'merged_states';
SELECT abs(polygonAreaCartesian(groupPolygonIntersectionMerge(s)) - (SELECT area FROM expected)) < 1e-6
FROM (SELECT id % 7 AS part, groupPolygonIntersectionState(p) AS s FROM deferred_octagons GROUP BY part);

SELECT 'state_round_trip';
SELECT abs(polygonAreaCartesian(finalizeAggregation(CAST(unhex(hex(s)) AS AggregateFunction(groupPolygonIntersection, Polygon))))
    - (SELECT area FROM expected)) < 1e-6
FROM (SELECT groupPolygonIntersectionState(p) AS s FROM (SELECT p FROM deferred_octagons ORDER BY id));

-- A row with a different exterior materializes the deferred state and continues on the general path.
SELECT 'exterior_switch';
SELECT abs(polygonAreaCartesian(groupPolygonIntersection(p))
    - (810000 - polygonAreaCartesian((SELECT groupPolygonUnion([hole]::Polygon) FROM deferred_octagons)))) < 1e-6
FROM
(
    SELECT p FROM
    (
        SELECT toFloat64(id) AS id, p FROM deferred_octagons
        UNION ALL
        SELECT 150.5, [[(0., 0.), (0., 900.), (900., 900.), (900., 0.), (0., 0.)]]::Polygon
    )
    ORDER BY id
);

-- Holes arranged in a closed ring leave an island: the result has two polygons.
SELECT 'island';
SELECT length(groupPolygonIntersection(p)),
    abs(polygonAreaCartesian(groupPolygonIntersection(p)) - (1000000 - polygonAreaCartesian(groupPolygonUnion(h)))) < 1e-6
FROM
(
    WITH arrayMap(k -> (500 + 20 * cos(number * 2 * pi() / 36) + 4 * cos(k * pi() / 4), 500 + 20 * sin(number * 2 * pi() / 36) + 4 * sin(k * pi() / 4)), range(8)) AS o,
        arrayPushBack(o, o[1]) AS ring
    SELECT [[(0., 0.), (0., 1000.), (1000., 1000.), (1000., 0.), (0., 0.)], ring]::Polygon AS p, [ring]::Polygon AS h
    FROM numbers(36)
);
