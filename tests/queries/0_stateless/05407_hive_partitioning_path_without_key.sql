-- Tags: no-fasttest
-- Tag no-fasttest: depends on S3

-- a path without the hive partition key causes INCORRECT_DATA, also when a filter on that key skips files

INSERT INTO FUNCTION file(currentDatabase() || '/05407/hour=10/a.csv', CSV, 'c1 Int64') SELECT 1 SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '/05407/hour=11/b.csv', CSV, 'c1 Int64') SELECT 2 SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '/05407/misc/stray.csv', CSV, 'c1 Int64') SELECT 3 SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION s3(s3_conn, filename = '05407_hive_partitioning_path_without_key/05407/hour=10/a.csv', format = CSV, structure = 'c1 Int64') SELECT 1 SETTINGS s3_truncate_on_insert = 1;
INSERT INTO FUNCTION s3(s3_conn, filename = '05407_hive_partitioning_path_without_key/05407/hour=11/b.csv', format = CSV, structure = 'c1 Int64') SELECT 2 SETTINGS s3_truncate_on_insert = 1;
INSERT INTO FUNCTION s3(s3_conn, filename = '05407_hive_partitioning_path_without_key/05407/misc/stray.csv', format = CSV, structure = 'c1 Int64') SELECT 3 SETTINGS s3_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '/05407_dup/key=1/key=2/data.csv', CSV, 'c1 Int64') SELECT 4 SETTINGS engine_file_truncate_on_insert = 1, use_hive_partitioning = 0;
INSERT INTO FUNCTION s3(s3_conn, filename = '05407_hive_partitioning_path_without_key/05407_dup/key=1/key=2/data.csv', format = CSV, structure = 'c1 Int64') SELECT 4 SETTINGS s3_truncate_on_insert = 1, use_hive_partitioning = 0;
INSERT INTO FUNCTION file(currentDatabase() || '/05407_headers/_headers=foo/data.csv', CSV, 'c1 Int64') SELECT 5 SETTINGS engine_file_truncate_on_insert = 1, use_hive_partitioning = 0;
INSERT INTO FUNCTION s3(s3_conn, filename = '05407_hive_partitioning_path_without_key/05407_headers/_headers=foo/data.csv', format = CSV, structure = 'c1 Int64') SELECT 5 SETTINGS s3_truncate_on_insert = 1, use_hive_partitioning = 0;

SET use_hive_partitioning = 1;

-- the brace alternatives keep the file order, so the first file hour=10/a.csv sets the hive columns
SELECT c1, hour FROM file(currentDatabase() || '/05407/{hour=10/a,hour=11/b}.csv') ORDER BY c1;
SELECT c1 FROM file(currentDatabase() || '/05407/{hour=10/a,hour=11/b,misc/stray}.csv') WHERE hour = 10; -- { serverError INCORRECT_DATA }
SELECT c1, hour FROM file(currentDatabase() || '/05407/{hour=10/a,hour=11/b,misc/stray}.csv') WHERE _file = 'a.csv';
SELECT c1, _file FROM file(currentDatabase() || '/05407/{hour=10/a,hour=11/b,misc/stray}.csv') WHERE _file = 'a.csv';

-- with an explicit structure, hour is a virtual column
SELECT c1, hour FROM file(currentDatabase() || '/05407/{hour=10/a,hour=11/b,misc/stray}.csv', CSV, 'c1 Int64') ORDER BY c1; -- { serverError INCORRECT_DATA }
SELECT c1 FROM file(currentDatabase() || '/05407/{hour=10/a,hour=11/b,misc/stray}.csv', CSV, 'c1 Int64') WHERE hour = 10; -- { serverError INCORRECT_DATA }

SELECT c1 FROM s3(s3_conn, filename = '05407_hive_partitioning_path_without_key/05407/*/*.csv', format = CSV) WHERE hour = 10; -- { serverError INCORRECT_DATA }
SELECT c1, hour FROM s3(s3_conn, filename = '05407_hive_partitioning_path_without_key/05407/*/*.csv', format = CSV) WHERE _file = 'a.csv';
SELECT c1, _file FROM s3(s3_conn, filename = '05407_hive_partitioning_path_without_key/05407/*/*.csv', format = CSV) WHERE _file = 'a.csv';
SELECT c1, hour FROM s3(s3_conn, filename = '05407_hive_partitioning_path_without_key/05407/*/*.csv', format = CSV, structure = 'c1 Int64') ORDER BY c1; -- { serverError INCORRECT_DATA }
SELECT c1 FROM s3(s3_conn, filename = '05407_hive_partitioning_path_without_key/05407/*/*.csv', format = CSV, structure = 'c1 Int64') WHERE hour = 10; -- { serverError INCORRECT_DATA }

-- s3Cluster gives all hive columns to the file filter, and the filter uses only the ones that it reads
-- the S3 paths do not use currentDatabase(): with it, the s3Cluster replicas do not find the hive columns
SELECT c1 FROM s3Cluster('test_cluster_one_shard_three_replicas_localhost', s3_conn, filename = '05407_hive_partitioning_path_without_key/05407/*/*.csv', format = CSV) WHERE hour = 10; -- { serverError INCORRECT_DATA }
SELECT c1, _file FROM s3Cluster('test_cluster_one_shard_three_replicas_localhost', s3_conn, filename = '05407_hive_partitioning_path_without_key/05407/*/*.csv', format = CSV) WHERE _file = 'a.csv';
SELECT c1, hour FROM s3Cluster('test_cluster_one_shard_three_replicas_localhost', s3_conn, filename = '05407_hive_partitioning_path_without_key/05407/*/*.csv', format = CSV) WHERE _file = 'a.csv';

-- a table created with use_hive_partitioning = 1 keeps its hive virtual columns when the setting is off
CREATE TABLE t_05407_s3 (c1 Int64) ENGINE = S3(s3_conn, filename = '05407_hive_partitioning_path_without_key/05407/hour=1*/*.csv', format = CSV);
SET use_hive_partitioning = 0;
SELECT c1, hour FROM t_05407_s3 ORDER BY c1;
SELECT c1 FROM t_05407_s3 WHERE hour = 10;

-- with use_hive_partitioning = 0, a read without hive columns does not parse the path, and key=1/key=2/ is valid
SELECT c1 FROM file(currentDatabase() || '/05407_dup/key=1/key=2/data.csv', CSV, 'c1 Int64');
SELECT c1, _file FROM file(currentDatabase() || '/05407_dup/key=1/key=2/data.csv', CSV, 'c1 Int64');
SELECT c1 FROM s3(s3_conn, filename = '05407_hive_partitioning_path_without_key/05407_dup/key=1/key=2/data.csv', format = CSV, structure = 'c1 Int64');
SELECT c1, _file FROM s3(s3_conn, filename = '05407_hive_partitioning_path_without_key/05407_dup/key=1/key=2/data.csv', format = CSV, structure = 'c1 Int64');

-- outside web sources, _headers is a hive partition key like any other
SET use_hive_partitioning = 1;
SELECT c1, _headers FROM file(currentDatabase() || '/05407_headers/_headers=foo/data.csv', CSV, 'c1 Int64');
SELECT c1 FROM file(currentDatabase() || '/05407_headers/_headers=foo/data.csv', CSV, 'c1 Int64') WHERE _headers = 'foo';
SELECT c1, _headers FROM s3(s3_conn, filename = '05407_hive_partitioning_path_without_key/05407_headers/_headers=foo/data.csv', format = CSV, structure = 'c1 Int64');
SELECT c1 FROM s3(s3_conn, filename = '05407_hive_partitioning_path_without_key/05407_headers/_headers=foo/data.csv', format = CSV, structure = 'c1 Int64') WHERE _headers = 'foo';
