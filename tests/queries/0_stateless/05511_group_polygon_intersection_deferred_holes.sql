-- Rows with a common exterior intersect to that exterior minus the union of their holes. Once the
-- state is large, rows with overlapping non-rectangular holes are intersected in size tiers instead
-- of one by one. Check the area against an independent `groupPolygonUnion` of the holes.

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

-- Large partial states are deferred themselves, so merging joins their hole unions.
SELECT 'merged_deferred_states';
SELECT abs(polygonAreaCartesian(groupPolygonIntersectionMerge(s)) - (SELECT area FROM expected)) < 1e-6
FROM (SELECT id % 2 AS part, groupPolygonIntersectionState(p) AS s FROM deferred_octagons GROUP BY part);

-- A running window finalizes the state after every row and keeps adding rows to it.
SELECT 'window';
SELECT countIf(abs(polygonAreaCartesian(w) - (1000000 - polygonAreaCartesian(u))) >= 1e-6)
FROM
(
    SELECT
        groupPolygonIntersection(p) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS w,
        groupPolygonUnion([hole]::Polygon) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS u
    FROM deferred_octagons WHERE id < 100
);

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

-- Holes arranged in a closed ring leave an island: the result has two polygons. The ring is large
-- enough for the state to cross the deferral threshold before it closes.
SELECT 'island';
SELECT length(groupPolygonIntersection(p)),
    abs(polygonAreaCartesian(groupPolygonIntersection(p)) - (1000000 - polygonAreaCartesian(groupPolygonUnion(h)))) < 1e-6
FROM
(
    WITH arrayMap(k -> (500 + 60 * cos(number * 2 * pi() / 108) + 4 * cos(k * pi() / 4), 500 + 60 * sin(number * 2 * pi() / 108) + 4 * sin(k * pi() / 4)), range(8)) AS o,
        arrayPushBack(o, o[1]) AS ring
    SELECT [[(0., 0.), (0., 1000.), (1000., 1000.), (1000., 0.), (0., 0.)], ring]::Polygon AS p, [ring]::Polygon AS h
    FROM numbers(108)
);

-- Holes that touch at points, touch the exterior inside an edge, or enclose an island must give the
-- same geometry as intersecting the rows one by one.
SELECT 'holes_one_ulp_apart';
WITH arrayConcat([(0.,0.),(0.,10.),(10.,10.),(10.,0.)], arrayMap(i -> (10.*i/301, 0.), range(300,0,-1)), [(0.,0.)]) AS e,
     [e, [(6.,5.),(7.,5.),(7.,5.999999999999999),(6.,5.999999999999999),(6.,5.)], [(6.,6.),(8.,6.),(8.,7.),(6.,7.),(6.,6.)]]::Polygon AS p
SELECT round(polygonAreaCartesian(groupPolygonIntersection(q)), 6), length(groupPolygonIntersection(q)) FROM (SELECT arrayJoin([p, p]) AS q) SETTINGS max_threads = 1;
SELECT 'hole_touching_exterior_edge';
WITH arrayConcat([(0.,0.),(0.,12.8),(0.,13.6),(0.,64.)], arrayMap(k -> (k/4, 64.), range(1,256)), [(64.,64.),(64.,0.),(0.,0.)]) AS e
SELECT round(polygonAreaCartesian(groupPolygonIntersection(p)), 6), length(groupPolygonIntersection(p)) FROM (
  SELECT arrayJoin([[e, [(0.,13.),(6.,7.),(12.,13.),(6.,19.),(0.,13.)]], [e, [(5.,14.),(7.,14.),(7.,42.),(33.,42.),(33.,14.),(35.,14.),(35.,44.),(5.,44.),(5.,14.)]]])::Polygon AS p) SETTINGS max_threads = 1;
SELECT 'holes_enclosing_island';
WITH arrayConcat([(0.,0.),(0.,12.8),(0.,13.6),(0.,64.)], arrayMap(k -> (k/4, 64.), range(1,256)), [(64.,64.),(64.,0.),(0.,0.)]) AS e
SELECT round(polygonAreaCartesian(groupPolygonIntersection(p)), 6), length(groupPolygonIntersection(p)) FROM (
  SELECT arrayJoin([[e, [(20.,20.),(20.,21.),(24.,21.),(24.,20.),(20.,20.)]], [e, [(20.,21.),(22.,23.),(24.,21.),(22.,22.),(20.,21.)]]])::Polygon AS p) SETTINGS max_threads = 1;
SELECT 'corner_touching_cells_enclosing_island';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(p)), 6), length(groupPolygonIntersection(p)) FROM (
  SELECT readWKTPolygon(concat('POLYGON((-1 -1,-1 100,100 100,100 -1,-1 -1),(', h, '))')) AS p
  FROM (SELECT arrayJoin(arrayConcat(
      arrayMap(k -> format('{0} 80,{1} 80,{1} 81,{0} 81,{0} 80', toString(k*1.5), toString(k*1.5+1)), range(55)),
      ['11 10,12 10,12 11,11 11,11 10','10 11,11 11,11 12,10 12,10 11',
       '12 11,13 11,13 12,12 12,12 11','11 12,12 12,12 13,11 13,11 12'])) AS h)) SETTINGS max_threads = 1;
