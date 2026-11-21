-- Tags: no-fasttest
-- Tag no-fasttest: requires S3

-- `bucket` granularity splits a file only when the format registers a bucket splitter.
-- `CSV` does not. Both files must still be read, one task each.

INSERT INTO FUNCTION s3('http://localhost:11111/test/' || currentDatabase() || '_05564_a.csv', NOSIGN, 'CSV', 'x UInt32') SELECT number FROM numbers(3) SETTINGS s3_truncate_on_insert = 1;
INSERT INTO FUNCTION s3('http://localhost:11111/test/' || currentDatabase() || '_05564_b.csv', NOSIGN, 'CSV', 'x UInt32') SELECT 10 SETTINGS s3_truncate_on_insert = 1;

SELECT sum(x), count()
FROM s3Cluster('test_shard_localhost', 'http://localhost:11111/test/' || currentDatabase() || '_05564_{a,b}.csv', NOSIGN, 'CSV', 'x UInt32')
SETTINGS cluster_table_function_split_granularity = 'bucket';
