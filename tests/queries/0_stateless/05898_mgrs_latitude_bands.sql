-- Covers the latitude band -> minimum northing table that MGRSToGeo consults while decoding
-- (getMinNorthing in src/Functions/UTMCoordinates.cpp). It selects the 2000 km northing cycle the
-- band lies in, so a wrong entry would move the decoded point by a multiple of 2000 km (~18 degrees)
-- and out of the band of the string it came from.
-- Spec: the MGRS latitude bands are the 8 degree tall letters C..X counted from 80S, omitting I and O,
-- with X widened to 72..84 (BAND_LETTERS in src/Functions/UTMCoordinates.cpp); geoToMGRS is defined for
-- latitudes in [-80, 84] and MGRSToGeo returns the centre of the referenced square, which is 1 m wide at
-- the default precision 5 -- see the FunctionDocumentation blocks of geoToMGRS and MGRSToGeo.

SELECT '-- every latitude band decodes back to the point it was encoded from';
SELECT lat,
       extract(m, '^[0-9]+([C-X])') AS band,
       geoToUTM(MGRSToGeo(m).1, MGRSToGeo(m).2).4 AS decoded_band,
       abs(MGRSToGeo(m).1 - (-60.0)) < 1e-4 AND abs(MGRSToGeo(m).2 - lat) < 1e-4 AS ok
FROM (
    -- longitude -60 is zone 21 in every band, so no Norway or Svalbard zone exception applies
    SELECT lat, geoToMGRS(-60.0, lat) AS m
    FROM values('lat Float64', -76, -68, -60, -52, -44, -36, -28, -20, -12, -4, 4, 12, 20, 28, 36, 44, 52, 60, 68, 78)
)   -- the centre latitude of each of the twenty bands C..X, in order
ORDER BY lat;

SELECT '-- a band letter outside C..X is rejected before the band table is consulted';
SELECT MGRSToGeo('21IAA'); -- { serverError BAD_ARGUMENTS } -- 'I' is omitted from the band sequence
SELECT MGRSToGeo('21OAA'); -- { serverError BAD_ARGUMENTS } -- 'O' is omitted as well
SELECT MGRSToGeo('21BAA'); -- { serverError BAD_ARGUMENTS } -- below the C..X range
SELECT MGRSToGeo('21YAA'); -- { serverError BAD_ARGUMENTS } -- above the C..X range
