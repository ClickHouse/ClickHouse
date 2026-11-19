-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3
-- https://github.com/ClickHouse/ClickHouse/pull/129592#discussion_r4620484936

SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM s3(untuple(tuple('http://127.0.0.1:1/bucket/file.csv', 'NOSIGN', 'CSV', 'a UInt8')), partition_strategy = (SELECT 'none')));
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM url(untuple(tuple('http://127.0.0.1:1/', 'CSV', 'a UInt8')), headers('X-A' = (SELECT 'b'))));
SELECT * FROM remote('localhost', untuple(tuple('system', 'one')), cityHash64(assumeNotNull(dummy + (SELECT 1))));
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM executable(untuple(tuple('nonexist.sh', 'TSV', 'x UInt8')), (SELECT 1)));
