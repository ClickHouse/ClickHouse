-- Tags: no-fasttest
-- Tag no-fasttest: needs `s3` and `icebergLocal`

-- `CREATE` checks the columns of a table `AS` a file-like table function (05567) without building the storage of the
-- function, which would list the files for the hive partition columns, or read the metadata of a data lake.
-- https://github.com/ClickHouse/ClickHouse/issues/129062

SELECT '--- the files of a path with globs are not listed';
-- The host is not reachable, so listing the files would retry for a long time.
CREATE TABLE t_s3_glob (a UInt8, _path String, k String) AS s3('http://127.0.0.1:1/bucket/*.csv', 'CSV', 'a UInt8');
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_s3_glob' ORDER BY position;

SELECT '--- without globs, the hive partition columns are those of the path';
CREATE TABLE t_s3_path (a UInt8, k String) AS s3('http://127.0.0.1:1/bucket/k=v/f.csv', 'CSV', 'a UInt8');
CREATE TABLE t_s3_missing (a UInt8, x String) AS s3('http://127.0.0.1:1/bucket/k=v/f.csv', 'CSV', 'a UInt8'); -- { serverError THERE_IS_NO_COLUMN }

SELECT '--- the hive partition columns of a pattern of alternatives are those of its first address, which needs no request';
CREATE TABLE t_s3_pattern (a UInt8, k String) AS s3('http://127.0.0.1:1/bucket/{k=v1,k=v2}/f.csv', 'CSV', 'a UInt8');
CREATE TABLE t_s3_pattern_missing (a UInt8, j String) AS s3('http://127.0.0.1:1/bucket/{k=v1,k=v2}/f.csv', 'CSV', 'a UInt8'); -- { serverError THERE_IS_NO_COLUMN }

SELECT '--- a placeholder is not a glob, and a partition strategy has no hive partition columns';
CREATE TABLE t_s3_placeholder (a UInt8, k String) AS s3('http://127.0.0.1:1/bucket/{_partition_id}.csv', 'CSV', 'a UInt8'); -- { serverError THERE_IS_NO_COLUMN }
CREATE TABLE t_s3_strategy (a UInt8, k String) AS s3('http://127.0.0.1:1/bucket/k=v/f.csv', 'CSV', 'a UInt8', partition_strategy = 'hive'); -- { serverError THERE_IS_NO_COLUMN }

SELECT '--- the response headers of url';
CREATE TABLE t_url (a UInt8, _headers Map(LowCardinality(String), LowCardinality(String))) AS url('http://127.0.0.1:1/f.csv', 'CSV', 'a UInt8');
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_url' ORDER BY position;
CREATE TABLE t_url_missing (a UInt8, x String) AS url('http://127.0.0.1:1/f.csv', 'CSV', 'a UInt8'); -- { serverError THERE_IS_NO_COLUMN }
-- A query string is not a glob.
CREATE TABLE t_url_query (a UInt8, x String) AS url('http://127.0.0.1:1/f.csv?x=1', 'CSV', 'a UInt8'); -- { serverError THERE_IS_NO_COLUMN }
CREATE TABLE t_url_pattern (a UInt8, k String) AS url('http://127.0.0.1:1/{k=v1,k=v2}/f.csv', 'CSV', 'a UInt8');
CREATE TABLE t_url_pattern_missing (a UInt8, j String) AS url('http://127.0.0.1:1/{k=v1,k=v2}/f.csv', 'CSV', 'a UInt8'); -- { serverError THERE_IS_NO_COLUMN }
-- A table reads a `*` in the path literally, without listing index pages, so it is not a glob either.
CREATE TABLE t_url_star (a UInt8, k String) AS url('http://127.0.0.1:1/k=v/*.csv', 'CSV', 'a UInt8');
CREATE TABLE t_url_star_missing (a UInt8, x UInt8) AS url('http://127.0.0.1:1/*.csv', 'CSV', 'a UInt8'); -- { serverError THERE_IS_NO_COLUMN }

SELECT '--- a data lake that has no metadata yet';
CREATE TABLE t_iceberg (a UInt8, _path String) AS icebergLocal(currentDatabase() || '_05568_ice', 'Parquet', 'a UInt8');
-- The storage of a data lake takes its columns from the metadata of the lake, which may have columns outside the structure,
-- so any column is accepted.
CREATE TABLE t_iceberg_other (a UInt8, k String) AS icebergLocal(currentDatabase() || '_05568_ice', 'Parquet', 'a UInt8');

SELECT '--- a column of a data lake outside the structure';
SET allow_insert_into_iceberg = 1;
SET iceberg_delete_data_on_drop = 1;
CREATE TABLE t_lake_writer (a Int64, b String) ENGINE = IcebergLocal(
    (SELECT concat(if(startsWith(value, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')), value, '/', currentDatabase(), '_05568_lake')
    FROM system.server_settings WHERE name = 'user_files_path'), 'Parquet');
INSERT INTO t_lake_writer SETTINGS async_insert = 0 VALUES (7, 'x');
CREATE TABLE t_lake (a Int64, b String) AS icebergLocal(currentDatabase() || '_05568_lake', 'Parquet', 'a Int64');
SELECT a, b FROM t_lake;

DROP TABLE t_s3_glob;
DROP TABLE t_s3_path;
DROP TABLE t_s3_pattern;
DROP TABLE t_url;
DROP TABLE t_url_pattern;
DROP TABLE t_url_star;
DROP TABLE t_iceberg;
DROP TABLE t_iceberg_other;
DROP TABLE t_lake;
DROP TABLE t_lake_writer;
