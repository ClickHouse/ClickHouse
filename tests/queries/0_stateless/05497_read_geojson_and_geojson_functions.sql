-- All geometry types, there and back. Extra coordinates and members are ignored, and the order of members does not matter.
SELECT g, variantType(g), geoJSON(g)
FROM
(
    SELECT readGeoJSON(arrayJoin([
        '{"type":"Point","coordinates":[1,2]}',
        ' {"coordinates" : [1.5,2.5,100] , "crs":{"x":1}, "type":"Point"}  ',
        '{"type":"MultiPoint","coordinates":[[1,2],[3,4]]}',
        '{"type":"LineString","coordinates":[[0,0],[1,1]]}',
        '{"type":"MultiLineString","coordinates":[[[0,0],[1,1]],[[2,2],[3,3]]]}',
        '{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]],[[0.1,0.1],[0.2,0.1],[0.2,0.2],[0.1,0.1]]]}',
        '{"type":"MultiPolygon","coordinates":[[[[0,0],[1,0],[1,1],[0,0]]]]}',
        'null'])) AS g
);

SELECT toTypeName(readGeoJSON('null')), toTypeName(geoJSON((1., 2.))), toTypeName(geoJSON(readWKT('POINT (1 2)')));
SELECT ST_GeomFromGeoJSON('{"type":"Point","coordinates":[1,2]}'), ST_AsGeoJSON((1., 2.));

SELECT '-- geoJSON of typed values';
SELECT geoJSON((1., 2.));
SELECT geoJSON([(1., 2.)]::MultiPoint);
SELECT geoJSON([(0., 0.), (1., 1.)]::LineString);
SELECT geoJSON([[(0., 0.), (1., 1.)]]::MultiLineString);
SELECT geoJSON([[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]::Polygon);
SELECT geoJSON([[[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]]]::MultiPolygon);
-- GeoJSON has no `Ring`, and this is what an array of points without a type name is.
SELECT geoJSON([(0., 0.), (1., 0.), (1., 1.), (0., 0.)]::Ring);
SELECT geoJSON([(0., 0.), (1., 0.), (1., 1.), (0., 0.)]);
SELECT geoJSON(NULL::Geometry), toTypeName(geoJSON(NULL::Geometry));
SELECT geoJSON(toNullable((1., 2.))), geoJSON(NULL::Nullable(Point));
SELECT number, geoJSON(if(number % 2 = 0, readWKT('POINT (1 2)'), readWKT('LINESTRING (1 2, 3 4)'))) FROM numbers(3);
-- Numbers are not quoted, and keep their precision.
SELECT geoJSON((1.5, 0.30000000000000004)) SETTINGS output_format_json_quote_64bit_floats = 1;
SELECT geoJSON(readGeoJSON('{"type":"Point","coordinates":[0.30000000000000004,1e300]}'));

SELECT '-- NULL';
SELECT s, readGeoJSON(s) FROM (SELECT arrayJoin(['{"type":"Point","coordinates":[1,2]}', NULL]) AS s) ORDER BY s;
SELECT readGeoJSON(NULL), toTypeName(readGeoJSON(NULL));
SELECT readGeoJSON(toLowCardinality(toNullable('{"type":"Point","coordinates":[1,2]}')));

SELECT '-- JSON, Dynamic and Tuple';
SELECT readGeoJSON('{"type":"LineString","coordinates":[[0,0],[1.5,1]]}'::JSON);
-- Integer coordinates must not be quoted when the object is converted to text.
SELECT readGeoJSON('{"type":"Point","coordinates":[9007199254740992,1]}'::JSON) SETTINGS output_format_json_quote_64bit_integers = 1;
SELECT number, variantType(g), g
FROM
(
    SELECT number, readGeoJSON(j.^geometry) AS g
    FROM
    (
        SELECT number, arrayElement([
            '{"geometry":{"type":"Point","coordinates":[1,2]}}',
            '{"geometry":null}',
            '{"id":1}',
            '{"geometry":{"type":"LineString","coordinates":[[0,0],[1,1.5]]}}'], number + 1)::JSON AS j
        FROM numbers(4)
    )
);
-- A string in a `JSON` column.
SELECT readGeoJSON(j.geometry), toTypeName(readGeoJSON(j.geometry)) FROM (SELECT '{"geometry":"{\\"type\\":\\"Point\\",\\"coordinates\\":[1,2]}"}'::JSON AS j);
SELECT readGeoJSON(d) FROM (SELECT arrayJoin(['{"type":"Point","coordinates":[1,2]}'::Dynamic, NULL::Dynamic, '{"type":"Point","coordinates":[3,4]}'::JSON::Dynamic]) AS d);
SELECT readGeoJSON(CAST(('Point', [0.5, 2]), 'Tuple(type String, coordinates Array(Decimal(10, 3)))')) SETTINGS output_format_json_quote_decimals = 1;
-- A geometry object in a `JSON` column is a sub-object, not a path.
SELECT readGeoJSON(j.geometry), readGeoJSON(j.^geometry) FROM (SELECT '{"geometry":{"type":"Point","coordinates":[1,2]}}'::JSON AS j);
-- The type that schema inference gives for a geometry in JSON Lines.
SELECT readGeoJSON(geometry) FROM format(JSONEachRow, '{"geometry":{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]}}')
SETTINGS input_format_json_try_infer_named_tuples_from_objects = 1, schema_inference_make_columns_nullable = 1;

SELECT '-- settings';
SELECT geoJSON([]::LineString), readGeoJSON('{"type":"LineString","coordinates":[]}') SETTINGS format_geojson_validate_geometry = 0;
SELECT readGeoJSON('{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,2]}]}') SETTINGS input_format_geojson_unsupported_geometry_handling = 'null';

SELECT '-- errors';
SELECT readGeoJSON('{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,2]}]}') SETTINGS input_format_geojson_unsupported_geometry_handling = 'throw'; -- { serverError INCORRECT_DATA }
SELECT readGeoJSON(''); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT readGeoJSON('{}'); -- { serverError INCORRECT_DATA }
SELECT readGeoJSON('{"type":"Point"}'); -- { serverError INCORRECT_DATA }
SELECT readGeoJSON('{"type":"Point","coordinates":[1]}'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT readGeoJSON('{"type":"Point","coordinates":[1e400,2]}'); -- { serverError INCORRECT_DATA }
SELECT readGeoJSON('{"type":"Point","coordinates":[1,2]} x'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT readGeoJSON('{"type":"Ring","coordinates":[[1,2]]}'); -- { serverError INCORRECT_DATA }
SELECT readGeoJSON('{"type":"Feature","geometry":null,"properties":{}}'); -- { serverError INCORRECT_DATA }
SELECT readGeoJSON('{"type":"LineString","coordinates":[[1,2]]}') SETTINGS format_geojson_validate_geometry = 1; -- { serverError INCORRECT_DATA }
SELECT readGeoJSON('{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[2,2]]]}') SETTINGS format_geojson_validate_geometry = 1; -- { serverError INCORRECT_DATA }
SELECT readGeoJSON('{"type":"GeometryCollection","geometries":' || repeat('[{"type":"GeometryCollection","geometries":', 100000)); -- { serverError TOO_DEEP_RECURSION }
SELECT readGeoJSON(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT readGeoJSON((1., 2.)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT geoJSON('x'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT geoJSON((1, 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT geoJSON((nan, 1.)); -- { serverError BAD_ARGUMENTS }
SELECT geoJSON((inf, 1.)); -- { serverError BAD_ARGUMENTS }
SELECT geoJSON([]::LineString) SETTINGS format_geojson_validate_geometry = 1; -- { serverError BAD_ARGUMENTS }
SELECT geoJSON([]::Polygon) SETTINGS format_geojson_validate_geometry = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- LowCardinality';
SELECT readGeoJSON(x) FROM (SELECT arrayJoin(['{"type":"Point","coordinates":[1,2]}', NULL])::LowCardinality(Nullable(String)) AS x);
