-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- Exact, `*` and `?` reads of a key with a space and an escaped character select the same object.

INSERT INTO FUNCTION s3('http://localhost:11111/test/05630 dir/a b1.csv', 'test', 'testtest', 'CSV', 'x UInt32') SELECT 1 SETTINGS s3_truncate_on_insert = 1;
INSERT INTO FUNCTION s3('http://localhost:11111/test/05630 dir/a b%231.csv', 'test', 'testtest', 'CSV', 'x UInt32') SELECT 2 SETTINGS s3_truncate_on_insert = 1;

SELECT 'a b%231.csv', x, _file FROM s3('http://localhost:11111/test/05630 dir/a b%231.csv', 'test', 'testtest', 'CSV', 'x UInt32');
SELECT 'a b%23*.csv', x, _file FROM s3('http://localhost:11111/test/05630 dir/a b%23*.csv', 'test', 'testtest', 'CSV', 'x UInt32');
SELECT 'a b%23?.csv', x, _file FROM s3('http://localhost:11111/test/05630 dir/a b%23?.csv', 'test', 'testtest', 'CSV', 'x UInt32');
SELECT 'a b?.csv', x, _file FROM s3('http://localhost:11111/test/05630 dir/a b?.csv', 'test', 'testtest', 'CSV', 'x UInt32');
SELECT 'a%20b?.csv', x, _file FROM s3('http://localhost:11111/test/05630 dir/a%20b?.csv', 'test', 'testtest', 'CSV', 'x UInt32');
