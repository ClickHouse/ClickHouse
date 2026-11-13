-- Tags: no-fasttest, need-query-parameters
-- Tag no-fasttest: depends on S3

-- A Hive partition value that the type of its partition column cannot hold is a conversion error, and NULL for a `Nullable` type.

SET use_hive_partitioning = 1;

INSERT INTO FUNCTION file(currentDatabase() || '/05628/k0=255/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 1 AS c0;
INSERT INTO FUNCTION file(currentDatabase() || '/05628/k0=256/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 2 AS c0;
INSERT INTO FUNCTION file(currentDatabase() || '/05628/k0=-1/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 3 AS c0;
INSERT INTO FUNCTION file(currentDatabase() || '/05628/k0=[256]/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 4 AS c0;
INSERT INTO FUNCTION file(currentDatabase() || '/05628/v/k0=1/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 5 AS c0;
INSERT INTO FUNCTION file(currentDatabase() || '/05628/v/k0=99999999999999999999/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 6 AS c0;

SELECT c0, k0 FROM file(currentDatabase() || '/05628/k0=255/data.jsonl', JSONEachRow, 'c0 Int32, k0 UInt8');
SELECT c0, k0 FROM file(currentDatabase() || '/05628/k0=256/data.jsonl', JSONEachRow, 'c0 Int32, k0 UInt8'); -- { serverError TYPE_MISMATCH }
SELECT c0, k0 FROM file(currentDatabase() || '/05628/k0=-1/data.jsonl', JSONEachRow, 'c0 Int32, k0 UInt8'); -- { serverError TYPE_MISMATCH }
SELECT c0, k0 FROM file(currentDatabase() || '/05628/k0=[256]/data.jsonl', JSONEachRow, 'c0 Int32, k0 Array(UInt8)'); -- { serverError TYPE_MISMATCH }
SELECT c0, k0 FROM file(currentDatabase() || '/05628/k0=256/data.jsonl', JSONEachRow, 'c0 Int32, k0 LowCardinality(UInt8)') SETTINGS allow_suspicious_low_cardinality_types = 1; -- { serverError TYPE_MISMATCH }
SELECT c0, k0 FROM file(currentDatabase() || '/05628/k0=256/data.jsonl', JSONEachRow, 'c0 Int32, k0 Nullable(UInt8)');
SELECT c0, k0 FROM file(currentDatabase() || '/05628/k0=256/data.jsonl', JSONEachRow, 'c0 Int32, k0 LowCardinality(Nullable(UInt8))') SETTINGS allow_suspicious_low_cardinality_types = 1;

-- `k0` is not in the structure, so its type is inferred from the first path of the list: `Int64`.
SELECT c0, k0 FROM file(currentDatabase() || '/05628/v/k0={1,99999999999999999999}/data.jsonl', JSONEachRow, 'c0 Int32') ORDER BY c0; -- { serverError TYPE_MISMATCH }

INSERT INTO FUNCTION s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05628/k0=256/data.jsonl', format = JSONEachRow) SETTINGS s3_truncate_on_insert = 1 SELECT 2 AS c0;
SELECT c0, k0 FROM s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05628/k0=256/data.jsonl', format = JSONEachRow, structure = 'c0 Int32, k0 UInt8'); -- { serverError TYPE_MISMATCH }
