-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

SELECT * FROM url('http://127.0.0.1:8123/?query=SELECT 42 FORMAT CSV', CSV, 'x UInt32');

INSERT INTO FUNCTION s3('http://localhost:11111/test/05407 dir/a b.csv', 'test', 'testtest', 'CSV', 'x UInt32') SELECT 1 SETTINGS s3_truncate_on_insert = 1;
INSERT INTO FUNCTION s3('http://localhost:11111/test/05407 dir/c d.csv.gz', 'test', 'testtest', 'CSV', 'x UInt32') SELECT 2 SETTINGS s3_truncate_on_insert = 1;
INSERT INTO FUNCTION s3('http://localhost:11111/test/05407 dir/e f.csv', 'test', 'testtest', 'CSV', 'x UInt32') SELECT 3 SETTINGS s3_truncate_on_insert = 1;

SELECT 's3';
SELECT x, _path, _file FROM s3('http://localhost:11111/test/05407 dir/a b.csv', 'test', 'testtest', 'CSV', 'x UInt32');
SELECT x, _file FROM s3('http://localhost:11111/test/05407%20dir/a%20b.csv', 'test', 'testtest', 'CSV', 'x UInt32');
SELECT x, _file FROM s3('http://localhost:11111/test/05407 dir/c d.csv.gz', 'test', 'testtest', 'CSV', 'x UInt32');
SELECT x, _file FROM s3('http://localhost:11111/test/05407 dir/{a b,c d}.csv*', 'test', 'testtest', 'CSV', 'x UInt32') ORDER BY x;
SELECT x FROM s3Cluster('test_cluster_one_shard_three_replicas_localhost', 'http://localhost:11111/test/05407 dir/a b.csv', 'test', 'testtest', 'CSV', 'x UInt32');

SELECT 'url';
SELECT x, _file FROM url('http://localhost:11111/test/05407 dir/a b.csv', 'CSV', 'x UInt32');
-- Format and compression are detected from a file name with spaces.
SELECT * FROM url('http://localhost:11111/test/05407 dir/a b.csv');
SELECT * FROM url('http://localhost:11111/test/05407 dir/c d.csv.gz');
SELECT x FROM url('http://localhost:11111/test/05407 dir/{a b,e f}.csv', 'CSV', 'x UInt32') ORDER BY x;
SELECT x FROM url('http://localhost:11111/test/05407 dir/a b.csv', 'CSV', 'x UInt32') SETTINGS enable_url_encoding = 0;
SELECT x FROM urlCluster('test_cluster_one_shard_three_replicas_localhost', 'http://localhost:11111/test/05407 dir/a b.csv', 'CSV', 'x UInt32');
-- `_file` and `_path` predicates see the same values the reader returns.
SELECT x, _file FROM url('http://localhost:11111/test/05407 dir/a b.csv', 'CSV', 'x UInt32') WHERE _file = 'a%20b.csv';
SELECT x, _path FROM url('http://localhost:11111/test/05407 dir/{a b,e f}.csv', 'CSV', 'x UInt32') WHERE _path = '/test/05407%20dir/e%20f.csv';
SELECT x, _file FROM url('http://localhost:11111/test/05407 dir/{a b,e f}.csv', 'CSV', 'x UInt32') WHERE _file = 'a b.csv' SETTINGS enable_url_encoding = 1;
SELECT x FROM urlCluster('test_cluster_one_shard_three_replicas_localhost', 'http://localhost:11111/test/05407 dir/{a b,e f}.csv', 'CSV', 'x UInt32') WHERE _file = 'e%20f.csv';

SELECT 'URL engine';
DROP TABLE IF EXISTS t_05407_url;
CREATE TABLE t_05407_url (x UInt32) ENGINE = URL('http://localhost:11111/test/05407 dir/a b.csv', 'CSV');
SELECT x FROM t_05407_url;
-- The URL is kept as written.
SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = 't_05407_url';
DROP TABLE t_05407_url;
