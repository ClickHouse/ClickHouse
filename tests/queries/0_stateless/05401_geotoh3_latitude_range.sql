-- Tags: no-fasttest
-- no-fasttest: geoToH3 needs a binary with the Uber H3 library
-- Tests for the latitude range check in geoToH3 (#126431).

-- Valid latitude, longitude - works and is unchanged:
SELECT geoToH3(55.71290588, 37.79506683, 15);

-- Boundary latitudes are accepted:
SELECT geoToH3(-90.0, 0.0, 1);
SELECT geoToH3(90.0, 0.0, 1);

-- Latitude 120 (out of range) must not silently produce a valid-looking cell (#126431):
SELECT geoToH3(120.0, 30.0, 5); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- Same under the legacy (lon, lat) argument order, where the second argument is latitude:
SELECT geoToH3(30.0, 120.0, 5) SETTINGS geotoh3_argument_order = 'lon_lat'; -- { serverError ARGUMENT_OUT_OF_BOUND }
