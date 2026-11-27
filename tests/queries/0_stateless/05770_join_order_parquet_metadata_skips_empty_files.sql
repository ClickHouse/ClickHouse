-- Tags: no-fasttest
-- Tag no-fasttest: needs S3

-- The estimate of a read of plain Parquet files from their footers skips the empty files, as the read does
-- (`s3_skip_empty_files`), instead of failing on their missing footers.

SET use_statistics = 1;
SET use_parquet_metadata_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_randomize = 0; -- Pinned because the test asserts on the estimated rows
SET s3_truncate_on_insert = 1;
SET s3_skip_empty_files = 1;

INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05770/data.parquet', format = Parquet, structure = 'id UInt64, d String')
SELECT number, toString(number % 10) FROM numbers(1000);

-- An empty object next to the Parquet file.
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05770/empty.parquet', format = RawBLOB, structure = 'x String')
SELECT '';

SELECT 'A glob with an empty file';
SELECT count() FROM s3(s3_conn, filename = currentDatabase() || '/05770/*.parquet', format = Parquet, structure = 'id UInt64, d String') AS a
JOIN s3(s3_conn, filename = currentDatabase() || '/05770/*.parquet', format = Parquet, structure = 'id UInt64, d String') AS b ON a.d = b.d;

SELECT 'The rows of the files';
SELECT extract(arrayStringConcat(groupArray(explain), '\n'), 'Left: rows estimated ([^\n]+)')
FROM (EXPLAIN SELECT count() FROM s3(s3_conn, filename = currentDatabase() || '/05770/*.parquet', format = Parquet, structure = 'id UInt64, d String') AS a
JOIN s3(s3_conn, filename = currentDatabase() || '/05770/*.parquet', format = Parquet, structure = 'id UInt64, d String') AS b ON a.d = b.d);

SELECT 'A directly named empty file';
SELECT count() FROM s3(s3_conn, filename = currentDatabase() || '/05770/empty.parquet', format = Parquet, structure = 'id UInt64, d String') AS a
JOIN s3(s3_conn, filename = currentDatabase() || '/05770/data.parquet', format = Parquet, structure = 'id UInt64, d String') AS b ON a.d = b.d;
