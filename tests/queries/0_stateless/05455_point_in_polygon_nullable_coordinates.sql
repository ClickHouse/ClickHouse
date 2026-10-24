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
