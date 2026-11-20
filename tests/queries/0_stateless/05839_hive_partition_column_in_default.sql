-- Tags: no-fasttest
-- Tag no-fasttest: uses the S3 mock server on localhost:11111

INSERT INTO FUNCTION file(currentDatabase() || '/k=abc/f.jsonl', LineAsString, 's String') SETTINGS engine_file_truncate_on_insert = 1 VALUES ('{"c0":1}'), ('{"c0":2,"d":"x"}');
INSERT INTO FUNCTION file(currentDatabase() || '/k=abc/f.csv', LineAsString, 's String') SETTINGS engine_file_truncate_on_insert = 1 VALUES ('1,'), ('2,x');
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/k=abc/f.jsonl', format = LineAsString, structure = 's String') SETTINGS s3_truncate_on_insert = 1 VALUES ('{"c0":1}'), ('{"c0":2,"d":"x"}');

SET use_hive_partitioning = 1;

SELECT * FROM file(currentDatabase() || '/k=abc/f.jsonl', JSONEachRow, 'c0 Int32, k String, d String DEFAULT k') ORDER BY c0;
SELECT d FROM file(currentDatabase() || '/k=abc/f.jsonl', JSONEachRow, 'c0 Int32, k String, d String DEFAULT k') ORDER BY d;
SELECT * FROM file(currentDatabase() || '/k=abc/f.jsonl', JSONEachRow, 'c0 Int32, k String, e String DEFAULT k, d String DEFAULT concat(e, \'!\')') ORDER BY c0;
SELECT * FROM file(currentDatabase() || '/k=abc/f.jsonl', JSONEachRow, 'c0 Int32, k Nullable(String), d String DEFAULT toString(k.null)') ORDER BY c0;
SELECT * FROM file(currentDatabase() || '/k=abc/f.csv', CSV, 'c0 Int32, k String DEFAULT \'none\', d String DEFAULT k') ORDER BY c0;
SELECT * FROM s3(s3_conn, filename = currentDatabase() || '/k=abc/f.jsonl', format = JSONEachRow, structure = 'c0 Int32, k String, d String DEFAULT k') ORDER BY c0;
SELECT * FROM url('http://localhost:11111/test/' || currentDatabase() || '/k=abc/f.jsonl', JSONEachRow, 'c0 Int32, k String, d String DEFAULT k') ORDER BY c0;
