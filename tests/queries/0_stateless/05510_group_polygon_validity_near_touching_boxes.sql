-- Two components whose bounding boxes touch, but whose boundaries are a few ULPs apart, have
-- intersecting interiors under the tolerance of `Boost.Geometry`. The validator must reject such
-- input instead of accepting it and letting the overlay silently drop a component.

SELECT polygonAreaCartesian(groupPolygonIntersection(p))
FROM (SELECT arrayJoin([
    readWKTMultiPolygon('MULTIPOLYGON(((2 1,2 4,3 4,3 1,2 1)),((1.9999999999999996 4,2 8,5 8,5 4.0000000000000018,1.9999999999999996 4)))'),
    readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 10,4.5 10,4.5 0,0 0)))')]) AS p)
SETTINGS max_threads = 1; -- { serverError BAD_ARGUMENTS }

SELECT polygonAreaCartesian(groupPolygonUnion(p))
FROM (SELECT readWKTMultiPolygon('MULTIPOLYGON(((2 1,2 4,3 4,3 1,2 1)),((1.9999999999999996 4,2 8,5 8,5 4.0000000000000018,1.9999999999999996 4)))') AS p); -- { serverError BAD_ARGUMENTS }

-- Boxes separated by a clear gap still skip the pairwise check and stay valid.
SELECT polygonAreaCartesian(groupPolygonUnion(p))
FROM (SELECT readWKTMultiPolygon('MULTIPOLYGON(((2 1,2 4,3 4,3 1,2 1)),((2 4.5,2 8,5 8,5 4.5,2 4.5)))') AS p);
