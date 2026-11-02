-- { echo }
-- Tags: no-fasttest
-- Tag no-fasttest: depends on S3

-- The type of a hive partition column is inferred from one sampled path. A sampled value that does not determine
-- a type (`NULL`, `null`, `[]`, an empty value) must give `LowCardinality(String)` like any other string, not a type
-- with `Nothing`, which reads every value of the column as NULL or fails.

SET use_hive_partitioning = 1;

INSERT INTO FUNCTION file(currentDatabase() || '/05407/p0=NULL/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 1 AS rid;
INSERT INTO FUNCTION file(currentDatabase() || '/05407/p0=x/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 2 AS rid;
INSERT INTO FUNCTION file(currentDatabase() || '/05407/p0=y/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 3 AS rid;
INSERT INTO FUNCTION file(currentDatabase() || '/05407/p0=null/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 4 AS rid;
INSERT INTO FUNCTION file(currentDatabase() || '/05407/p0=[]/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 5 AS rid;
INSERT INTO FUNCTION file(currentDatabase() || '/05407/p0=/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 6 AS rid;
INSERT INTO FUNCTION file(currentDatabase() || '/05407/p0=()/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 7 AS rid;
INSERT INTO FUNCTION file(currentDatabase() || '/05407/variant/a/p0=[(NULL,1),1]/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 8 AS rid;
INSERT INTO FUNCTION file(currentDatabase() || '/05407/variant/b/p0=[(true,2),1]/data.jsonl', JSONEachRow) SETTINGS engine_file_truncate_on_insert = 1 SELECT 9 AS rid;

-- The first path of the brace list is the sampled one.
SELECT rid, p0, toTypeName(p0) FROM file(currentDatabase() || '/05407/p0={NULL,x,y}/data.jsonl') ORDER BY rid;
SELECT rid, p0, toTypeName(p0) FROM file(currentDatabase() || '/05407/p0={x,y,NULL}/data.jsonl') ORDER BY rid;
SELECT rid, p0, toTypeName(p0) FROM file(currentDatabase() || '/05407/p0={null,x,y}/data.jsonl') ORDER BY rid;
SELECT count() FROM file(currentDatabase() || '/05407/p0={NULL,x,y}/data.jsonl') WHERE p0 = 'x';
SELECT rid, p0, toTypeName(p0) FROM file(currentDatabase() || '/05407/p0={NULL,x,y}/data.jsonl') ORDER BY rid SETTINGS schema_inference_make_columns_nullable = 0;
SELECT rid, p0, toTypeName(p0) FROM file(currentDatabase() || '/05407/p0={[],x}/data.jsonl') ORDER BY rid;
SELECT rid, p0, toTypeName(p0) FROM file(currentDatabase() || '/05407/p0{=,=x}/data.jsonl') ORDER BY rid;
SELECT rid, p0, toTypeName(p0) FROM file(currentDatabase() || '/05407/p0={(),x}/data.jsonl') ORDER BY rid;
-- With variant inference `[(NULL,1),1]` gives `Array(Variant(Int64, Tuple(Nullable(Nothing), ...)))`.
SELECT rid, p0, toTypeName(p0) FROM file(currentDatabase() || '/05407/variant/{a,b}/p0=*/data.jsonl') ORDER BY rid SETTINGS input_format_try_infer_variants = 1;

-- S3 lists keys in lexicographic order, so `p0=NULL` is sampled.
-- A query parameter, not `currentDatabase()`: a parallel replica evaluates `currentDatabase()` in its default database.
INSERT INTO FUNCTION s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05407/p0=NULL/data.jsonl', format = JSONEachRow) SETTINGS s3_truncate_on_insert = 1 SELECT 1 AS rid;
INSERT INTO FUNCTION s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05407/p0=x/data.jsonl', format = JSONEachRow) SETTINGS s3_truncate_on_insert = 1 SELECT 2 AS rid;
INSERT INTO FUNCTION s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05407/p0=y/data.jsonl', format = JSONEachRow) SETTINGS s3_truncate_on_insert = 1 SELECT 3 AS rid;
SELECT rid, p0, toTypeName(p0) FROM s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05407/p0=*/data.jsonl', format = JSONEachRow) ORDER BY rid;
SELECT count() FROM s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05407/p0=*/data.jsonl', format = JSONEachRow) WHERE p0 = 'x';
