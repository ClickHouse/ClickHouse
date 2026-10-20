-- Areas of polygons and rings must not depend on winding direction.
SELECT areaCartesian(readWKTPolygon('POLYGON((0 0,0 4,4 4,4 0,0 0))'));
SELECT areaCartesian(readWKTPolygon('POLYGON((0 0,4 0,4 4,0 4,0 0))'));
SELECT areaCartesian(readWKTRing('POLYGON((0 0,4 0,4 4,0 4,0 0))'));
SELECT areaCartesian(readWKT('POLYGON((0 0,4 0,4 4,0 4,0 0))'));

-- Holes subtract area even if the outer and inner rings are reversed or share winding.
SELECT areaCartesian(readWKTPolygon('POLYGON((0 0,0 4,4 4,4 0,0 0),(1 1,2 1,2 2,1 2,1 1))'));
SELECT areaCartesian(readWKTPolygon('POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,1 2,2 2,2 1,1 1))'));
SELECT areaCartesian(readWKTPolygon('POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))'));

-- Opposite polygon winding within a MultiPolygon must not cancel areas.
SELECT areaCartesian(readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 4,4 4,4 0,0 0)),((6 0,10 0,10 4,6 4,6 0)))'));
SELECT areaCartesian(readWKT('MULTIPOLYGON(((0 0,0 4,4 4,4 0,0 0)),((6 0,10 0,10 4,6 4,6 0)))'));
SELECT areaCartesian(readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 4,4 4,4 0,0 0),(1 1,2 1,2 2,1 2,1 1)),((6 0,10 0,10 4,6 4,6 0)))'));

-- Spherical areas must obey the same winding and hole rules.
SELECT round(areaSpherical(readWKTPolygon('POLYGON((0 0,0 1,1 1,1 0,0 0))')), 6);
SELECT round(areaSpherical(readWKTPolygon('POLYGON((0 0,1 0,1 1,0 1,0 0))')), 6);
SELECT round(areaSpherical(readWKTPolygon('POLYGON((0 0,0 4,4 4,4 0,0 0),(1 1,2 1,2 2,1 2,1 1))')), 6);
SELECT round(areaSpherical(readWKTPolygon('POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,1 2,2 2,2 1,1 1))')), 6);
SELECT round(areaSpherical(readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)),((2 0,3 0,3 1,2 1,2 0)))')), 6);
SELECT round(areaSpherical(readWKT('MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)),((2 0,3 0,3 1,2 1,2 0)))')), 6);

-- Non-area geometries keep their zero result.
SELECT areaCartesian(readWKTPoint('POINT(0 0)')), areaCartesian(readWKTLineString('LINESTRING(0 0,1 1)'));
