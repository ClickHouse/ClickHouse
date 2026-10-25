-- Nullable point coordinates propagate NULL through both constant and non-constant polygon paths.

SELECT id, constant_polygon, nonconstant_polygon, toTypeName(constant_polygon)
FROM
(
    SELECT
        id,
        pointInPolygon((x, y), [(0., 0.), (1., 0.), (1., 1.), (0., 1.)]) AS constant_polygon,
        pointInPolygon((x, y), materialize([(0., 0.), (1., 0.), (1., 1.), (0., 1.)])) AS nonconstant_polygon
    FROM values('id UInt8, x Nullable(Float64), y Nullable(Float64)',
        (1, 0.5, 0.5),
        (2, 1.5, 0.5),
        (3, NULL, 0.5),
        (4, 0.5, NULL),
        (5, NULL, NULL))
)
ORDER BY id;

-- Constant nullable points and a single nullable coordinate keep a nullable result.
SELECT pointInPolygon((toNullable(0.5), toNullable(0.5)), [(0., 0.), (1., 0.), (1., 1.), (0., 1.)]) AS result, toTypeName(result);
SELECT pointInPolygon((CAST(NULL AS Nullable(Float64)), toNullable(0.5)), [(0., 0.), (1., 0.), (1., 1.), (0., 1.)]) AS result, toTypeName(result);
SELECT pointInPolygon((toFloat64(0.5), toNullable(0.5)), [(0., 0.), (1., 0.), (1., 1.), (0., 1.)]) AS result, toTypeName(result);

-- Non-nullable input still has the original UInt8 result type.
SELECT pointInPolygon((0.5, 0.5), [(0., 0.), (1., 0.), (1., 1.), (0., 1.)]) AS result, toTypeName(result);

-- Nullable polygon vertices are still rejected.
SELECT pointInPolygon((0.5, 0.5), [(toNullable(0.), 0.), (toNullable(1.), 0.), (toNullable(1.), 1.)]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Nullable point coordinates with polygon holes and multiple polygons.
-- Cover both cached constant and row-wise non-constant polygon algorithms.
SELECT
    id,
    pointInPolygon((x, y), [[(0., 0.), (4., 0.), (4., 4.), (0., 4.)], [(1., 1.), (3., 1.), (3., 3.), (1., 3.)]]) AS const_holes,
    pointInPolygon((x, y), materialize([[(0., 0.), (4., 0.), (4., 4.), (0., 4.)], [(1., 1.), (3., 1.), (3., 3.), (1., 3.)]])) AS dynamic_holes,
    pointInPolygon((x, y), [[[(0., 0.), (1., 0.), (1., 1.), (0., 1.)]], [[(5., 5.), (6., 5.), (6., 6.), (5., 6.)]]]) AS const_multi,
    pointInPolygon((x, y), materialize([[[(0., 0.), (1., 0.), (1., 1.), (0., 1.)]], [[(5., 5.), (6., 5.), (6., 6.), (5., 6.)]]])) AS dynamic_multi,
    toTypeName(const_holes)
FROM values('id UInt8, x Nullable(Float64), y Nullable(Float64)',
    (1, 0.5, 0.5), (2, 2., 2.), (3, 5.5, 5.5),
    (4, NULL, 0.5), (5, 0.5, NULL), (6, NULL, NULL))
ORDER BY id;

-- Constant NULL points preserve NULL across multiple rows in both polygon paths.
SELECT
    number,
    pointInPolygon((CAST(NULL AS Nullable(Float64)), toNullable(0.5)), [(0., 0.), (1., 0.), (1., 1.), (0., 1.)]) AS const_result,
    pointInPolygon((CAST(NULL AS Nullable(Float64)), toNullable(0.5)), materialize([(0., 0.), (1., 0.), (1., 1.), (0., 1.)])) AS dynamic_result,
    toTypeName(const_result), toTypeName(dynamic_result)
FROM numbers(2)
ORDER BY number;

-- Multiple polygon arguments preserve nullable results.
SELECT pointInPolygon((toNullable(0.5), toNullable(0.5)),
    [(0., 0.), (4., 0.), (4., 4.), (0., 4.)], [(1., 1.), (3., 1.), (3., 3.), (1., 3.)]) AS result, toTypeName(result);
SELECT pointInPolygon((toNullable(5.5), toNullable(5.5)),
    [[(0., 0.), (1., 0.), (1., 1.), (0., 1.)]], [[(5., 5.), (6., 5.), (6., 6.), (5., 6.)]]) AS result, toTypeName(result);
SELECT pointInPolygon((CAST(NULL AS Nullable(Float64)), toNullable(5.5)),
    [[(0., 0.), (1., 0.), (1., 1.), (0., 1.)]], [[(5., 5.), (6., 5.), (6., 6.), (5., 6.)]]) AS result, toTypeName(result);

-- Nullable integer coordinates are supported too.
SELECT pointInPolygon((CAST(1 AS Nullable(UInt8)), CAST(1 AS Nullable(UInt8))),
    [(0., 0.), (2., 0.), (2., 2.), (0., 2.)]) AS result, toTypeName(result);

SELECT
    number,
    pointInPolygon(toNullable((CAST(NULL AS Nullable(Float64)), toNullable(0.5))), [(0., 0.), (1., 0.), (1., 1.), (0., 1.)]) AS const_ring,
    pointInPolygon(toNullable((CAST(NULL AS Nullable(Float64)), toNullable(0.5))), [[[(0., 0.), (1., 0.), (1., 1.), (0., 1.)]]]) AS const_multi,
    pointInPolygon(toNullable((CAST(NULL AS Nullable(Float64)), toNullable(0.5))), materialize([(0., 0.), (1., 0.), (1., 1.), (0., 1.)])) AS dynamic_ring,
    pointInPolygon(toNullable((CAST(NULL AS Nullable(Float64)), toNullable(0.5))), materialize([[[(0., 0.), (1., 0.), (1., 1.), (0., 1.)]]])) AS dynamic_multi,
    toTypeName(const_ring), toTypeName(const_multi)
FROM numbers(2)
ORDER BY number;

SELECT pointInPolygon(toNullable((toNullable(0.5), CAST(NULL AS Nullable(Float64)))), [(0., 0.), (1., 0.), (1., 1.), (0., 1.)]) AS result, toTypeName(result);
SELECT pointInPolygon(toNullable((toNullable(0.5), toNullable(0.5))), [(0., 0.), (1., 0.), (1., 1.), (0., 1.)]) AS result, toTypeName(result);
