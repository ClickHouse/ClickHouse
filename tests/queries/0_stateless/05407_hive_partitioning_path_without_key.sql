-- Tags: no-fasttest
-- Tag no-fasttest: depends on S3

-- a path without the hive partition key gives INCORRECT_DATA, also when a filter prunes the files by that key

INSERT INTO FUNCTION file(currentDatabase() || '/05407/hour=10/a.csv', CSV, 'c1 Int64') SELECT 1 SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '/05407/hour=11/b.csv', CSV, 'c1 Int64') SELECT 2 SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '/05407/misc/stray.csv', CSV, 'c1 Int64') SELECT 3 SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05407/hour=10/a.csv', format = CSV, structure = 'c1 Int64') SELECT 1 SETTINGS s3_truncate_on_insert = 1;
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05407/hour=11/b.csv', format = CSV, structure = 'c1 Int64') SELECT 2 SETTINGS s3_truncate_on_insert = 1;
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05407/misc/stray.csv', format = CSV, structure = 'c1 Int64') SELECT 3 SETTINGS s3_truncate_on_insert = 1;

SET use_hive_partitioning = 1;

-- the brace alternatives keep the file order, so the hive columns come from hour=10/a.csv
SELECT c1, hour FROM file(currentDatabase() || '/05407/{hour=10/a,hour=11/b}.csv') ORDER BY c1;
SELECT c1 FROM file(currentDatabase() || '/05407/{hour=10/a,hour=11/b,misc/stray}.csv') WHERE hour = 10; -- { serverError INCORRECT_DATA }
SELECT c1 FROM file(currentDatabase() || '/05407/{hour=10/a,hour=11/b,misc/stray}.csv') WHERE hour IN (10, 11); -- { serverError INCORRECT_DATA }
SELECT c1, hour FROM file(currentDatabase() || '/05407/{hour=10/a,hour=11/b,misc/stray}.csv') WHERE _file = 'a.csv'; -- { serverError INCORRECT_DATA }
SELECT c1, _file FROM file(currentDatabase() || '/05407/{hour=10/a,hour=11/b,misc/stray}.csv') WHERE _file = 'a.csv';

-- with an explicit structure, hour is a virtual column
SELECT c1, hour FROM file(currentDatabase() || '/05407/{hour=10/a,hour=11/b,misc/stray}.csv', CSV, 'c1 Int64') ORDER BY c1; -- { serverError INCORRECT_DATA }
SELECT c1 FROM file(currentDatabase() || '/05407/{hour=10/a,hour=11/b,misc/stray}.csv', CSV, 'c1 Int64') WHERE hour = 10; -- { serverError INCORRECT_DATA }

SELECT c1, hour FROM s3(s3_conn, filename = currentDatabase() || '/05407/hour=1*/*.csv', format = CSV) ORDER BY c1;
SELECT c1 FROM s3(s3_conn, filename = currentDatabase() || '/05407/*/*.csv', format = CSV) WHERE hour = 10; -- { serverError INCORRECT_DATA }
SELECT c1, hour FROM s3(s3_conn, filename = currentDatabase() || '/05407/*/*.csv', format = CSV) WHERE _file = 'a.csv'; -- { serverError INCORRECT_DATA }
SELECT c1, _file FROM s3(s3_conn, filename = currentDatabase() || '/05407/*/*.csv', format = CSV) WHERE _file = 'a.csv';
SELECT c1, hour FROM s3(s3_conn, filename = currentDatabase() || '/05407/*/*.csv', format = CSV, structure = 'c1 Int64') ORDER BY c1; -- { serverError INCORRECT_DATA }
SELECT c1 FROM s3(s3_conn, filename = currentDatabase() || '/05407/*/*.csv', format = CSV, structure = 'c1 Int64') WHERE hour = 10; -- { serverError INCORRECT_DATA }

-- a table made with use_hive_partitioning = 1 keeps its hive virtual columns when the setting is off
CREATE TABLE t_05407_s3 (c1 Int64) ENGINE = S3(s3_conn, filename = currentDatabase() || '/05407/hour=1*/*.csv', format = CSV);
SET use_hive_partitioning = 0;
SELECT c1, hour FROM t_05407_s3 ORDER BY c1;
SELECT c1 FROM t_05407_s3 WHERE hour = 10;
