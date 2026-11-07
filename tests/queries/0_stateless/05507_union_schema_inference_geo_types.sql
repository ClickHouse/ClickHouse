-- Tags: no-fasttest
-- Tag no-fasttest: depends on Parquet support

SELECT toTypeName([readWkt('POINT(1 2)'), readWkt('POLYGON(((0 0, 1 0, 1 1, 0 0)))')]);
SELECT toTypeName(if(1, readWkt('POINT(1 2)'), readWkt('POLYGON(((0 0, 1 0, 1 1, 0 0)))')));
SELECT toTypeName(multiIf(0, readWkt('POINT(1 2)'), 1, readWkt('LINESTRING(0 0, 1 1)'), readWkt('POLYGON(((0 0, 1 0, 1 1, 0 0)))')));
SELECT toTypeName(coalesce(readWkt('POINT(1 2)'), readWkt('POLYGON(((0 0, 1 0, 1 1, 0 0)))')));

INSERT INTO FUNCTION file(currentDatabase() || '_05507_p1.parquet', 'Parquet', 'id UInt32, g Point') SELECT 1, (1., 2.);
INSERT INTO FUNCTION file(currentDatabase() || '_05507_p2.parquet', 'Parquet', 'id UInt32, g Polygon') SELECT 2, [[(0., 0.), (1., 0.), (1., 1.), (0., 0.)]];

SELECT name, type FROM (DESC file(currentDatabase() || '_05507_p{1,2}.parquet') SETTINGS schema_inference_mode = 'union', schema_inference_make_columns_nullable = 0) ORDER BY name;
SELECT id, toTypeName(g), g FROM file(currentDatabase() || '_05507_p{1,2}.parquet') SETTINGS schema_inference_mode = 'union', schema_inference_make_columns_nullable = 0 ORDER BY id;
