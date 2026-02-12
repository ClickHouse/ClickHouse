-- Benchmark NOAA GHCN Synthetic Geometry (30M rows)

-- 1. Warmup
SELECT count() FROM noaa_ghcn_geometry FORMAT Null;

-- 2. Ring Union (Triangle) - Grouped by Station (Parallel Aggr)
SELECT station_id, length(groupPolygonUnion(ring)) FROM noaa_ghcn_geometry GROUP BY station_id FORMAT Null;

-- 3. Polygon Union (Square) - Grouped by Station
SELECT station_id, length(groupPolygonUnion(polygon)) FROM noaa_ghcn_geometry GROUP BY station_id FORMAT Null;

-- 4. MultiPolygon Union (Two Squares) - Grouped by Station
SELECT station_id, length(groupPolygonUnion(multipolygon)) FROM noaa_ghcn_geometry GROUP BY station_id FORMAT Null;

-- 5. Ring Intersection (Grouped)
SELECT station_id, length(groupPolygonIntersection(ring)) FROM noaa_ghcn_geometry GROUP BY station_id FORMAT Null;

-- 6. Polygon Intersection (Grouped)
SELECT station_id, length(groupPolygonIntersection(polygon)) FROM noaa_ghcn_geometry GROUP BY station_id FORMAT Null;

-- 7. MultiPolygon Intersection (Grouped)
SELECT station_id, length(groupPolygonIntersection(multipolygon)) FROM noaa_ghcn_geometry GROUP BY station_id FORMAT Null;
