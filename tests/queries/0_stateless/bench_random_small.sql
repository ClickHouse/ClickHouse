
SELECT 'Warmup', count() FROM (SELECT * FROM noaa_ghcn_geometry LIMIT 100000);

SELECT 'Union Ring', count() FROM (SELECT station_id, length(groupPolygonUnion(ring)) FROM (SELECT * FROM noaa_ghcn_geometry LIMIT 100000) GROUP BY station_id);
SELECT 'Union Polygon', count() FROM (SELECT station_id, length(groupPolygonUnion(polygon)) FROM (SELECT * FROM noaa_ghcn_geometry LIMIT 100000) GROUP BY station_id);
SELECT 'Union MultiPolygon', count() FROM (SELECT station_id, length(groupPolygonUnion(multipolygon)) FROM (SELECT * FROM noaa_ghcn_geometry LIMIT 100000) GROUP BY station_id);

SELECT 'Intersection Ring', count() FROM (SELECT station_id, length(groupPolygonIntersection(ring)) FROM (SELECT * FROM noaa_ghcn_geometry LIMIT 100000) GROUP BY station_id);
SELECT 'Intersection Polygon', count() FROM (SELECT station_id, length(groupPolygonIntersection(polygon)) FROM (SELECT * FROM noaa_ghcn_geometry LIMIT 100000) GROUP BY station_id);
