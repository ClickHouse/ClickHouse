-- Tags: no-fasttest
-- no-fasttest: the Parquet format is not available in the fast test build.

-- The `metadata` and `value` of a `Parquet` `VARIANT` inside an array have a row per array element,
-- not per row of the file. A selective `PREWHERE` over an `Array(Dynamic)` must not apply the row
-- filter to them, and a directly read nested `JSON` path must size its metadata from the metadata
-- column's own rows.

SET output_format_parquet_use_custom_encoder = 1, engine_file_truncate_on_insert = 1, enable_json_type = 1;

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_arr.parquet')
SELECT arr FROM (SELECT 1 AS k, []::Array(Dynamic) AS arr UNION ALL SELECT 2, [42::Dynamic] UNION ALL SELECT 3, []::Array(Dynamic) UNION ALL SELECT 4, ['x'::Dynamic, 7::Dynamic, 42::Dynamic]) ORDER BY k;

SELECT arr FROM file(currentDatabase() || '_arr.parquet', Parquet, 'arr Array(Dynamic)') PREWHERE arrayExists(x -> toString(x) = '42', arr);
SELECT arr FROM file(currentDatabase() || '_arr.parquet', Parquet, 'arr Array(Dynamic)') WHERE arrayExists(x -> toString(x) = '42', arr);

INSERT INTO TABLE FUNCTION file(currentDatabase() || '_json.parquet')
SELECT j FROM (SELECT 1 AS k, '{"arr":[{"a":1},{"a":2}]}'::JSON AS j UNION ALL SELECT 2, '{"arr":[{"a":3},{"a":4}]}'::JSON) ORDER BY k
SETTINGS output_format_parquet_json_as_variant = 1;

SELECT `j.arr.a` FROM file(currentDatabase() || '_json.parquet', Parquet, '`j.arr.a` Array(Dynamic)');
