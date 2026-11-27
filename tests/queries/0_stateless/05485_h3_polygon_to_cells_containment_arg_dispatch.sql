-- Tags: no-fasttest
-- Tag no-fasttest: the fasttest build is compiled without USE_H3, so h3PolygonToCellsWithContainment is not registered there.

-- Argument validation and geometry-type dispatch of `h3PolygonToCellsWithContainment`.
-- Expected behaviour: the FunctionDocumentation block of src/Functions/h3PolygonToCellsWithContainment.cpp
-- (first argument Ring/Polygon/MultiPolygon, resolution 0..15, flags 0..3).

-- Resolving the return type only type-checks the flags, so a non-Array geometry and a non-UInt8 resolution
-- are both rejected later, while the columns are read.
SELECT h3PolygonToCellsWithContainment('abc', 7, 0); -- { serverError ILLEGAL_COLUMN }
SELECT h3PolygonToCellsWithContainment([(0., 0.), (0., 0.2), (0.2, 0.2)], 'abc', 0); -- { serverError ILLEGAL_COLUMN }

-- Resolution above the H3 maximum of 15.
SELECT h3PolygonToCellsWithContainment([(0., 0.), (0., 0.2), (0.2, 0.2)], 100, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- UInt32 flags have their own path (the value is read as is instead of being cast to Int64). Constant and
-- materialized flags are validated against 0..3 and give the same cells as the UInt8 literal.
SELECT arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCellsWithContainment([(0., 0.), (0., 0.2), (0.2, 0.2)], 5, toUInt32(2))));
SELECT h3PolygonToCellsWithContainment([(0., 0.), (0., 0.2), (0.2, 0.2)], 5, materialize(toUInt32(2)))
     = h3PolygonToCellsWithContainment([(0., 0.), (0., 0.2), (0.2, 0.2)], 5, 2);
SELECT h3PolygonToCellsWithContainment([(0., 0.), (0., 0.2), (0.2, 0.2)], 5, toUInt32(4)); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Geometries that are not areal are rejected by their custom type name, not by the underlying Array shape:
-- LineString and MultiPoint both resolve to Array(Tuple(Float64, Float64)), which is the shape of a Ring.
SELECT h3PolygonToCellsWithContainment(CAST([(1., 2.), (3., 4.)], 'LineString'), 7, 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT h3PolygonToCellsWithContainment(CAST([(1., 2.), (3., 4.)], 'MultiPoint'), 7, 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A Polygon is covered as a one-element MultiPolygon, so it yields the same cells as the equivalent
-- MultiPolygon, and an interior ring removes cells that the outer ring alone covers.
SELECT arraySort(arrayMap(x -> h3ToString(x), h3PolygonToCellsWithContainment(
    CAST([[(0., 0.), (0., 0.3), (0.3, 0.3), (0., 0.)], [(0.05, 0.06), (0.05, 0.2), (0.19, 0.2), (0.05, 0.06)]], 'Polygon'), 6, 0)));

WITH
    CAST([[(0., 0.), (0., 0.3), (0.3, 0.3), (0., 0.)]], 'Polygon') AS solid_polygon,
    CAST([[[(0., 0.), (0., 0.3), (0.3, 0.3), (0., 0.)]]], 'MultiPolygon') AS same_as_multi_polygon,
    CAST([[(0., 0.), (0., 0.3), (0.3, 0.3), (0., 0.)], [(0.05, 0.06), (0.05, 0.2), (0.19, 0.2), (0.05, 0.06)]], 'Polygon') AS holed_polygon
SELECT
    h3PolygonToCellsWithContainment(solid_polygon, 6, 0) = h3PolygonToCellsWithContainment(same_as_multi_polygon, 6, 0),
    length(h3PolygonToCellsWithContainment(solid_polygon, 6, 0)),
    length(h3PolygonToCellsWithContainment(holed_polygon, 6, 0)),
    hasAll(h3PolygonToCellsWithContainment(solid_polygon, 6, 0), h3PolygonToCellsWithContainment(holed_polygon, 6, 0));
