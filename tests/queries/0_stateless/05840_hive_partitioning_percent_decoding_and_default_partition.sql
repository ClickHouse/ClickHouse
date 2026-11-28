-- { echo }

-- Hive-style writers (pyarrow, DuckDB, Polars) percent-encode partition values in directory names and write NULL as
-- `__HIVE_DEFAULT_PARTITION__`. Values are read decoded (an invalid escape stays literal), and the marker reads as NULL,
-- or as the type default for a non-`Nullable` column, as `input_format_null_as_default` does for any input format.

SET use_hive_partitioning = 1;

INSERT INTO FUNCTION file(currentDatabase() || '/05840/p0=a%20b/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 1 AS rid;
INSERT INTO FUNCTION file(currentDatabase() || '/05840/p0=x%3Dy/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 2 AS rid;
INSERT INTO FUNCTION file(currentDatabase() || '/05840/p0=%C3%A9/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 3 AS rid;
INSERT INTO FUNCTION file(currentDatabase() || '/05840/p0=null/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 4 AS rid;
INSERT INTO FUNCTION file(currentDatabase() || '/05840/p0=__HIVE_DEFAULT_PARTITION__/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 5 AS rid;
INSERT INTO FUNCTION file(currentDatabase() || '/05840/p0=100%/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 6 AS rid;
INSERT INTO FUNCTION file(currentDatabase() || '/05840/p0=a+b%zz/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 7 AS rid;

SELECT rid, p0, toTypeName(p0) FROM file(currentDatabase() || '/05840/p0=*/data.jsonl') ORDER BY rid;
SELECT rid, p0, p0 IS NULL FROM file(currentDatabase() || '/05840/p0=*/data.jsonl', JSONEachRow, 'rid Int64, p0 Nullable(String)') ORDER BY rid;

-- The file-pruning filter sees the same values.
SELECT count() FROM file(currentDatabase() || '/05840/p0=*/data.jsonl') WHERE p0 = 'a b';
SELECT rid FROM file(currentDatabase() || '/05840/p0=*/data.jsonl', JSONEachRow, 'rid Int64, p0 Nullable(String)') WHERE p0 IS NULL;
SELECT rid FROM file(currentDatabase() || '/05840/p0=*/data.jsonl') WHERE p0 = 'é';

-- The marker alone does not determine a type, so a sampled marker infers a string.
SELECT rid, p0, toTypeName(p0) FROM file(currentDatabase() || '/05840/p0={__HIVE_DEFAULT_PARTITION__,a%20b}/data.jsonl') ORDER BY rid;

INSERT INTO FUNCTION file(currentDatabase() || '/05840/n=1/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 1 AS rid;
INSERT INTO FUNCTION file(currentDatabase() || '/05840/n=__HIVE_DEFAULT_PARTITION__/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 2 AS rid;
INSERT INTO FUNCTION file(currentDatabase() || '/05840/n=1e%2B3/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 3 AS rid;

SELECT rid, n FROM file(currentDatabase() || '/05840/n=*/data.jsonl', JSONEachRow, 'rid Int64, n Nullable(Float64)') ORDER BY rid;
SELECT rid, n FROM file(currentDatabase() || '/05840/n=*/data.jsonl', JSONEachRow, 'rid Int64, n Float64') ORDER BY rid;
SELECT rid, n FROM file(currentDatabase() || '/05840/n=*/data.jsonl', JSONEachRow, 'rid Int64, n Float64') ORDER BY rid SETTINGS input_format_null_as_default = 0; -- { serverError TYPE_MISMATCH }
