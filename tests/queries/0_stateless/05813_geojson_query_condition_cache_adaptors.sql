-- The query condition cache must tell apart the settings that `geoJSON` and `readGeoJSON` capture,
-- also when the `Variant` adaptor (for `Geometry`) or the `Dynamic` adaptor runs them.

SET use_query_condition_cache = 1;

DROP TABLE IF EXISTS t_geojson_cache;
CREATE TABLE t_geojson_cache (id UInt64, g Geometry, d Dynamic)
ENGINE = MergeTree ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;

INSERT INTO t_geojson_cache
SELECT number, []::LineString::Geometry, '{"type":"GeometryCollection","geometries":[]}'::Dynamic FROM numbers(100000);

SELECT count() FROM t_geojson_cache WHERE isNull(geoJSON(g)) SETTINGS format_geojson_validate_geometry = 0;
SELECT count() FROM t_geojson_cache WHERE isNull(geoJSON(g)) SETTINGS format_geojson_validate_geometry = 1; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM t_geojson_cache WHERE isNotNull(readGeoJSON(d)) SETTINGS input_format_geojson_unsupported_geometry_handling = 'null';
SELECT count() FROM t_geojson_cache WHERE isNotNull(readGeoJSON(d)) SETTINGS input_format_geojson_unsupported_geometry_handling = 'throw'; -- { serverError INCORRECT_DATA }

DROP TABLE t_geojson_cache;
