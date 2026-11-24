-- Tags: no-fasttest
-- Tag no-fasttest: Parquet and the S3 mock server on localhost:11111

INSERT INTO FUNCTION file(currentDatabase() || '/external.jsonl', JSONEachRow, 'external UInt32') SETTINGS engine_file_truncate_on_insert = 1 VALUES (1);
INSERT INTO FUNCTION file(currentDatabase() || '/external.parquet', Parquet, 'external UInt32, t Tuple(x UInt32)') SETTINGS engine_file_truncate_on_insert = 1 VALUES (1, tuple(3));
INSERT INTO FUNCTION file(currentDatabase() || '/external.csv', LineAsString, 's String') SETTINGS engine_file_truncate_on_insert = 1 VALUES ('1,');
-- Not async: an async insert flush evaluates `currentDatabase()` in the table function arguments without the session database.
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/external.jsonl', format = JSONEachRow, structure = 'external UInt32') SETTINGS async_insert = 0, s3_truncate_on_insert = 1 VALUES (1);

-- The query does not read the columns that the default of `next` uses.
SELECT next FROM file(currentDatabase() || '/external.jsonl', JSONEachRow, 'id UInt32 DEFAULT 5, next UInt32 DEFAULT id + 1');
SELECT next FROM file(currentDatabase() || '/external.jsonl', JSONEachRow, 'external UInt32, next UInt32 DEFAULT external + 1');
SELECT b FROM file(currentDatabase() || '/external.jsonl', JSONEachRow, 'a UInt32 DEFAULT external + 1, b UInt32 DEFAULT a + 1, external UInt32');
SELECT next FROM file(currentDatabase() || '/external.jsonl', JSONEachRow, 'external UInt32, t Tuple(x UInt32) DEFAULT tuple(external + 2), next UInt32 DEFAULT t.x + 1');
-- The file has `external`, so the default of its input `id` is not evaluated, also for CSV, which reads every column.
SELECT external FROM file(currentDatabase() || '/external.jsonl', JSONEachRow, 'external UInt32 DEFAULT id, id UInt32 DEFAULT toUInt32(materialize(\'bad\'))');
SELECT external FROM file(currentDatabase() || '/external.csv', CSV, 'external UInt32, id UInt32 DEFAULT toUInt32(materialize(\'bad\'))');
SELECT next FROM url('http://localhost:11111/test/' || currentDatabase() || '/external.jsonl', JSONEachRow, 'id UInt32 DEFAULT 5, next UInt32 DEFAULT id + 1');
SELECT next FROM s3(s3_conn, filename = currentDatabase() || '/external.jsonl', format = JSONEachRow, structure = 'id UInt32 DEFAULT 5, next UInt32 DEFAULT id + 1');

-- Parquet reads tuple elements on their own.
SELECT next FROM file(currentDatabase() || '/external.parquet', Parquet, 'external UInt32, t Tuple(x UInt32), next UInt32 DEFAULT t.x + external');
SELECT next FROM file(currentDatabase() || '/external.parquet', Parquet, 'external UInt32, t Tuple(x UInt32) DEFAULT tuple(0), next UInt32 DEFAULT t.x + 1');
SELECT t.x, next FROM file(currentDatabase() || '/external.parquet', Parquet, 'external UInt32, t Tuple(x UInt32) DEFAULT tuple(0), next UInt32 DEFAULT t.x + 1');
SELECT u.x FROM file(currentDatabase() || '/external.parquet', Parquet, 'external UInt32, u Tuple(x UInt32) DEFAULT tuple(external + 5)');

-- Only a virtual column is requested, so the smallest column `next` is read.
SELECT _file FROM file(currentDatabase() || '/external.jsonl', JSONEachRow, 'next UInt8 DEFAULT id + 1, id UInt32 DEFAULT 5') SETTINGS optimize_count_from_files = 0;
