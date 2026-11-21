-- Tags: no-fasttest, no-replicated-database, need-query-parameters
-- Tag no-fasttest: depends on S3
-- Tag no-replicated-database: every replica creates the queue on the same Keeper path

-- A direct SELECT from a queue takes its Hive partition columns from the object path, also when the file has the column.

SET enable_object_storage_queue_hive_partitioning = 1, s3_truncate_on_insert = 1;

INSERT INTO FUNCTION s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05813/k=abc/data.jsonl', format = JSONEachRow) SELECT 1 AS c0;
INSERT INTO FUNCTION s3(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05813/k=xyz/data.jsonl', format = JSONEachRow) SELECT 2 AS c0, 'file' AS k;

CREATE TABLE queue (c0 Int32, k String)
ENGINE = S3Queue(s3_conn, filename = {CLICKHOUSE_DATABASE:String} || '/05813/k=*/data.jsonl', format = JSONEachRow)
SETTINGS mode = 'unordered', keeper_path = '/clickhouse/{database}/05813_queue', use_hive_partitioning = 1;

SELECT c0, k FROM queue ORDER BY c0 SETTINGS stream_like_engine_allow_direct_select = 1;

DROP TABLE queue SYNC;
