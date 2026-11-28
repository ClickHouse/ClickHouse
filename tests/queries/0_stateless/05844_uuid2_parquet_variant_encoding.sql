-- Tags: no-fasttest
-- no-fasttest: Parquet support is not compiled into the fast-test build.

-- A `UUID2` value inside `Dynamic` or `Variant` is written into the Parquet variant column
-- with the same canonical bytes as `UUID`, and reads back as the same identifier.

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05844_dynamic.parquet')
    SELECT toUUID2('00112233-4455-6677-8899-aabbccddeeff')::Dynamic AS v2, toUUID('00112233-4455-6677-8899-aabbccddeeff')::Dynamic AS v1;
SELECT 'Dynamic';
SELECT toString(v2), toString(v1), toString(v2) = toString(v1) FROM file(currentDatabase() || '_05844_dynamic.parquet');

INSERT INTO FUNCTION file(currentDatabase() || '_05844_variant.parquet')
    SELECT arrayJoin(['00112233-4455-6677-8899-aabbccddeeff'::UUID2::Variant(UUID2, String), 'text'::Variant(UUID2, String)]) AS v;
SELECT 'Variant';
SELECT toString(v) FROM file(currentDatabase() || '_05844_variant.parquet') ORDER BY ALL;

INSERT INTO FUNCTION file(currentDatabase() || '_05844_array.parquet')
    SELECT [toUUID2('00112233-4455-6677-8899-aabbccddeeff'), toUUID2('ffeeddcc-bbaa-9988-7766-554433221100')]::Dynamic AS v;
SELECT 'Array';
SELECT toString(v) FROM file(currentDatabase() || '_05844_array.parquet');
