-- Regression test for MGRS encode/decode/re-encode consistency near the equator.

SELECT '-- MGRS re-encodes to the same string (precision 0 and 5)';
SELECT geoToMGRS(lon, lat, prec) = geoToMGRS(MGRSToGeo(geoToMGRS(lon, lat, prec)).1, MGRSToGeo(geoToMGRS(lon, lat, prec)).2, prec)
FROM values('lon Float64, lat Float64, prec UInt8', (-3., -1e-15, 0), (-3., -1e-15, 5), (-3., -1e-308, 0), (-3., -1e-308, 5), (-3., 0., 0), (-3., 0., 5), (-3., 4., 0), (-3., 4., 5));