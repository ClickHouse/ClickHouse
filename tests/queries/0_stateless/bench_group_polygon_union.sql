-- =============================================================================
-- Standalone performance benchmark for groupPolygonUnion
-- Run with: clickhouse-client --time --multiquery < bench_group_polygon_union.sql
--
-- Tags: no-fasttest, long
-- =============================================================================

SELECT '=== groupPolygonUnion Performance Benchmark ===';
SELECT '';

SET allow_experimental_analyzer = 1;

-- =============================================================================
-- 1. ROW COUNT SCALING
--    Measures how aggregate cost grows with input size.
-- =============================================================================

SELECT '--- Test 1: Row Count Scaling (Polygon input, simple 5-vertex squares) ---';

DROP TABLE IF EXISTS bench_scale;
CREATE TABLE bench_scale (n UInt32, polygon Polygon) ENGINE = Memory;

INSERT INTO bench_scale
SELECT
    number,
    [arrayMap(i -> (
        toFloat64(number % 500) + [0, 5, 5, 0, 0][i + 1],
        toFloat64(intDiv(number, 500)) + [0, 0, 5, 5, 0][i + 1]
    ), range(5))]
FROM numbers(500000);

SELECT '  1K rows:';
SELECT groupPolygonUnion(polygon) FROM bench_scale WHERE n < 1000 FORMAT Null;

SELECT '  10K rows:';
SELECT groupPolygonUnion(polygon) FROM bench_scale WHERE n < 10000 FORMAT Null;

SELECT '  100K rows:';
SELECT groupPolygonUnion(polygon) FROM bench_scale WHERE n < 100000 FORMAT Null;

SELECT '  500K rows:';
SELECT groupPolygonUnion(polygon) FROM bench_scale FORMAT Null;

DROP TABLE bench_scale;

SELECT '';

-- =============================================================================
-- 2. POLYGON COMPLEXITY SCALING
--    Measures cost as vertex count per polygon increases.
-- =============================================================================

SELECT '--- Test 2: Polygon Complexity Scaling (10K rows, varying vertex count) ---';

-- 4 vertices (square)
DROP TABLE IF EXISTS bench_complexity;
CREATE TABLE bench_complexity (polygon Polygon) ENGINE = Memory;
INSERT INTO bench_complexity
SELECT [arrayMap(i -> (
    toFloat64(number % 100) + cos(i / 2.0 * pi()) * 5,
    toFloat64(intDiv(number, 100)) + sin(i / 2.0 * pi()) * 5
), range(4))]
FROM numbers(10000);

SELECT '  4 vertices:';
SELECT groupPolygonUnion(polygon) FROM bench_complexity FORMAT Null;

-- 20 vertices
DROP TABLE bench_complexity;
CREATE TABLE bench_complexity (polygon Polygon) ENGINE = Memory;
INSERT INTO bench_complexity
SELECT [arrayMap(i -> (
    toFloat64(number % 100) + cos(i / 10.0 * pi()) * 5,
    toFloat64(intDiv(number, 100)) + sin(i / 10.0 * pi()) * 5
), range(20))]
FROM numbers(10000);

SELECT '  20 vertices:';
SELECT groupPolygonUnion(polygon) FROM bench_complexity FORMAT Null;

-- 100 vertices
DROP TABLE bench_complexity;
CREATE TABLE bench_complexity (polygon Polygon) ENGINE = Memory;
INSERT INTO bench_complexity
SELECT [arrayMap(i -> (
    toFloat64(number % 100) + cos(i / 50.0 * pi()) * 5,
    toFloat64(intDiv(number, 100)) + sin(i / 50.0 * pi()) * 5
), range(100))]
FROM numbers(10000);

SELECT '  100 vertices:';
SELECT groupPolygonUnion(polygon) FROM bench_complexity FORMAT Null;

-- 500 vertices
DROP TABLE bench_complexity;
CREATE TABLE bench_complexity (polygon Polygon) ENGINE = Memory;
INSERT INTO bench_complexity
SELECT [arrayMap(i -> (
    toFloat64(number % 100) + cos(i / 250.0 * pi()) * 5,
    toFloat64(intDiv(number, 100)) + sin(i / 250.0 * pi()) * 5
), range(500))]
FROM numbers(10000);

SELECT '  500 vertices:';
SELECT groupPolygonUnion(polygon) FROM bench_complexity FORMAT Null;

DROP TABLE bench_complexity;

SELECT '';

-- =============================================================================
-- 3. GROUP BY CARDINALITY
--    Measures performance across different numbers of aggregation groups.
-- =============================================================================

SELECT '--- Test 3: GROUP BY Cardinality (100K rows, varying group count) ---';

DROP TABLE IF EXISTS bench_group;
CREATE TABLE bench_group (grp UInt32, polygon Polygon) ENGINE = Memory;
INSERT INTO bench_group
SELECT
    number,
    [arrayMap(i -> (
        toFloat64(number % 500) + [0, 5, 5, 0, 0][i + 1],
        toFloat64(intDiv(number, 500)) + [0, 0, 5, 5, 0][i + 1]
    ), range(5))]
FROM numbers(100000);

SELECT '  10 groups (10K rows/group):';
SELECT grp % 10 AS g, groupPolygonUnion(polygon) FROM bench_group GROUP BY g FORMAT Null;

SELECT '  100 groups (1K rows/group):';
SELECT grp % 100 AS g, groupPolygonUnion(polygon) FROM bench_group GROUP BY g FORMAT Null;

SELECT '  1000 groups (100 rows/group):';
SELECT grp % 1000 AS g, groupPolygonUnion(polygon) FROM bench_group GROUP BY g FORMAT Null;

SELECT '  10000 groups (10 rows/group):';
SELECT grp % 10000 AS g, groupPolygonUnion(polygon) FROM bench_group GROUP BY g FORMAT Null;

DROP TABLE bench_group;

SELECT '';

-- =============================================================================
-- 4. INPUT TYPE COMPARISON
--    Compares Ring vs Polygon vs MultiPolygon at the same scale.
-- =============================================================================

SELECT '--- Test 4: Input Type Comparison (50K rows, simple squares) ---';

DROP TABLE IF EXISTS bench_ring;
DROP TABLE IF EXISTS bench_polygon;
DROP TABLE IF EXISTS bench_mpoly;

CREATE TABLE bench_ring (ring Ring) ENGINE = Memory;
CREATE TABLE bench_polygon (polygon Polygon) ENGINE = Memory;
CREATE TABLE bench_mpoly (mpoly MultiPolygon) ENGINE = Memory;

INSERT INTO bench_ring
SELECT arrayMap(i -> (
    toFloat64(number % 500) + [0, 5, 5, 0, 0][i + 1],
    toFloat64(intDiv(number, 500)) + [0, 0, 5, 5, 0][i + 1]
), range(5))
FROM numbers(50000);

INSERT INTO bench_polygon
SELECT [arrayMap(i -> (
    toFloat64(number % 500) + [0, 5, 5, 0, 0][i + 1],
    toFloat64(intDiv(number, 500)) + [0, 0, 5, 5, 0][i + 1]
), range(5))]
FROM numbers(50000);

INSERT INTO bench_mpoly
SELECT [[arrayMap(i -> (
    toFloat64(number % 500) + [0, 5, 5, 0, 0][i + 1],
    toFloat64(intDiv(number, 500)) + [0, 0, 5, 5, 0][i + 1]
), range(5))]]
FROM numbers(50000);

SELECT '  Ring:';
SELECT groupPolygonUnion(ring) FROM bench_ring FORMAT Null;

SELECT '  Polygon:';
SELECT groupPolygonUnion(polygon) FROM bench_polygon FORMAT Null;

SELECT '  MultiPolygon:';
SELECT groupPolygonUnion(mpoly) FROM bench_mpoly FORMAT Null;

DROP TABLE bench_ring;
DROP TABLE bench_polygon;
DROP TABLE bench_mpoly;

SELECT '';

-- =============================================================================
-- 5. OVERLAP RATIO
--    Compares fully overlapping, partially overlapping, and disjoint patterns.
-- =============================================================================

SELECT '--- Test 5: Overlap Pattern (50K rows) ---';

-- Fully overlapping: all identical polygons
DROP TABLE IF EXISTS bench_overlap;
CREATE TABLE bench_overlap (polygon Polygon) ENGINE = Memory;
INSERT INTO bench_overlap
SELECT [[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]]
FROM numbers(50000);

SELECT '  Fully overlapping (identical):';
SELECT groupPolygonUnion(polygon) FROM bench_overlap FORMAT Null;

-- Partially overlapping: shifted squares
DROP TABLE bench_overlap;
CREATE TABLE bench_overlap (polygon Polygon) ENGINE = Memory;
INSERT INTO bench_overlap
SELECT [arrayMap(i -> (
    toFloat64(number % 200) * 3 + [0, 5, 5, 0, 0][i + 1],
    toFloat64(intDiv(number, 200)) * 3 + [0, 0, 5, 5, 0][i + 1]
), range(5))]
FROM numbers(50000);

SELECT '  Partially overlapping:';
SELECT groupPolygonUnion(polygon) FROM bench_overlap FORMAT Null;

-- Fully disjoint: far apart
DROP TABLE bench_overlap;
CREATE TABLE bench_overlap (polygon Polygon) ENGINE = Memory;
INSERT INTO bench_overlap
SELECT [arrayMap(i -> (
    toFloat64(number) * 100 + [0, 1, 1, 0, 0][i + 1],
    toFloat64(0) + [0, 0, 1, 1, 0][i + 1]
), range(5))]
FROM numbers(5000);

SELECT '  Fully disjoint:';
SELECT groupPolygonUnion(polygon) FROM bench_overlap FORMAT Null;

DROP TABLE bench_overlap;

SELECT '';

-- =============================================================================
-- 6. SERIALIZATION COST
--    Measures the serialize/deserialize path by forcing distributed-like
--    two-level aggregation.
-- =============================================================================

SELECT '--- Test 6: Serialization Cost (two-level aggregation, 10K rows, 100 groups) ---';

DROP TABLE IF EXISTS bench_serde;
CREATE TABLE bench_serde (grp UInt32, polygon Polygon) ENGINE = Memory;
INSERT INTO bench_serde
SELECT
    number % 100 AS grp,
    [arrayMap(i -> (
        toFloat64(number % 100) + [0, 5, 5, 0, 0][i + 1],
        toFloat64(intDiv(number, 100)) + [0, 0, 5, 5, 0][i + 1]
    ), range(5))]
FROM numbers(10000);

-- Force two-level aggregation to trigger serialize/deserialize
SELECT '  Two-level aggregation:';
SELECT grp, groupPolygonUnion(polygon) FROM bench_serde GROUP BY grp
SETTINGS group_by_two_level_threshold = 1 FORMAT Null;

DROP TABLE bench_serde;

SELECT '';
SELECT '=== Benchmark complete ===';
