-- Tags: no-fasttest, need-query-parameters
-- Tag no-fasttest: depends on S3

-- A filter on a Hive partition column sees the partition value of a path as the column reads it, so it fails on a value the column cannot hold.

SET use_hive_partitioning = 1;

INSERT INTO FUNCTION file(currentDatabase() || '/05629/a/k0=300/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 1 AS c0;
INSERT INTO FUNCTION file(currentDatabase() || '/05629/a/k0=-1/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 2 AS c0;
INSERT INTO FUNCTION file(currentDatabase() || '/05629/a/k0=5/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 3 AS c0;
INSERT INTO FUNCTION file(currentDatabase() || '/05629/a/k0=7/data.jsonl', RawBLOB) SETTINGS engine_file_truncate_on_insert = 1 SELECT 'not json';
INSERT INTO FUNCTION file(currentDatabase() || '/05629/b/a=NULL/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 4 AS c0;
INSERT INTO FUNCTION file(currentDatabase() || '/05629/b/a=x/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 5 AS c0;

SELECT c0, k0 FROM file(currentDatabase() || '/05629/a/k0=300/data.jsonl', JSONEachRow, 'c0 Int32, k0 Nullable(UInt8)') WHERE k0 IS NULL;
SELECT c0, k0 FROM file(currentDatabase() || '/05629/a/k0=-1/data.jsonl', JSONEachRow, 'c0 Int32, k0 Nullable(UInt8)') WHERE k0 IS NULL;
SELECT c0, k0 FROM file(currentDatabase() || '/05629/a/k0=300/data.jsonl', JSONEachRow, 'c0 Int32, k0 LowCardinality(Nullable(UInt8))') WHERE k0 IS NULL SETTINGS allow_suspicious_low_cardinality_types = 1;
SELECT c0, a FROM file(currentDatabase() || '/05629/b/a={NULL,x}/data.jsonl', JSONEachRow, 'c0 Int32, a Nullable(String)') WHERE a IS NOT NULL ORDER BY c0;
SELECT c0, a FROM file(currentDatabase() || '/05629/b/a={NULL,x}/data.jsonl', JSONEachRow, 'c0 Int32, a Nullable(String)') WHERE a = 'NULL';
-- `k0=7/data.jsonl` is not JSON, so this reads only if the filter skips that path.
SELECT c0, k0 FROM file(currentDatabase() || '/05629/a/k0={5,7}/data.jsonl', JSONEachRow, 'c0 Int32, k0 UInt8') WHERE k0 = 5;
SELECT c0, k0 FROM file(currentDatabase() || '/05629/a/k0={300,5}/data.jsonl', JSONEachRow, 'c0 Int32, k0 UInt8') WHERE k0 = 5; -- { serverError TYPE_MISMATCH }

INSERT INTO FUNCTION s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05629/k0=300/data.jsonl', format = JSONEachRow) SETTINGS s3_truncate_on_insert = 1 SELECT 1 AS c0;
SELECT c0, k0 FROM s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05629/k0=300/data.jsonl', format = JSONEachRow, structure = 'c0 Int32, k0 Nullable(UInt8)') WHERE k0 IS NULL;
