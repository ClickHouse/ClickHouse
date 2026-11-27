-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- No such bucket, so an unfixed server cannot delete a real one. Root credentials: the `test` user may not delete buckets.
CREATE TABLE t_empty_key (c UInt8) ENGINE = S3('http://localhost:11111/test-05565-no-such-bucket/', 'clickhouse', 'clickhouse', 'CSV');
TRUNCATE TABLE t_empty_key; -- { serverError S3_ERROR }
DROP TABLE t_empty_key;
