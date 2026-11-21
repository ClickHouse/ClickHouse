-- When only some components of two large operands come close, `groupPolygonUnion` overlays just those and
-- keeps the others unchanged. The result must match the plain union: check areas and component counts.

SET max_threads = 1;

-- A diagonal chain of squares touching at corners stays one component per square.
SELECT 'corner_chain';
SELECT polygonAreaCartesian(groupPolygonUnion(p)), length(groupPolygonUnion(p))
FROM (SELECT [[(x, x), (x, x + 1), (x + 1, x + 1), (x + 1, x), (x, x)]]::Polygon AS p FROM (SELECT toFloat64(number) AS x FROM numbers(1000)));

-- A checkerboard touches at corners in both directions.
SELECT 'checkerboard';
SELECT polygonAreaCartesian(groupPolygonUnion(p)), length(groupPolygonUnion(p))
FROM
(
    SELECT [[(x, y), (x, y + 1), (x + 1, y + 1), (x + 1, y), (x, y)]]::Polygon AS p
    FROM (SELECT toFloat64(number % 40) AS x, toFloat64(intDiv(number, 40)) AS y FROM numbers(1600) WHERE (number % 40 + intDiv(number, 40)) % 2 = 0)
    ORDER BY cityHash64(x, y)
);

-- Pairs of overlapping squares in shuffled order merge into one rectangle per pair.
SELECT 'overlapping_pairs';
SELECT polygonAreaCartesian(groupPolygonUnion(p)), length(groupPolygonUnion(p))
FROM
(
    SELECT [[(x, 0.), (x, 1.), (x + 1, 1.), (x + 1, 0.), (x, 0.)]]::Polygon AS p
    FROM (SELECT 4. * intDiv(number, 2) + 0.5 * (number % 2) AS x FROM numbers(2000))
    ORDER BY cityHash64(x)
);

-- A long bar crosses a row of separate squares, merging all of them into one polygon with notches.
SELECT 'bar_across_squares';
SELECT polygonAreaCartesian(groupPolygonUnion(p)), length(groupPolygonUnion(p))
FROM
(
    SELECT [[(3. * number, 0.), (3. * number, 2.), (3. * number + 1, 2.), (3. * number + 1, 0.), (3. * number, 0.)]]::Polygon AS p FROM numbers(300)
    UNION ALL
    SELECT [[(0., 0.5), (0., 1.5), (900., 1.5), (900., 0.5), (0., 0.5)]]::Polygon
);
