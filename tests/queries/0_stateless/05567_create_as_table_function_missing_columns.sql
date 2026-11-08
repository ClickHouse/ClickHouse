-- A table created `AS` a table function reads the columns of the function's storage by name, so `CREATE` rejects a
-- stored column of the table that the storage does not resolve: such a table could never be read. Another type for the
-- same name is converted, and an `ALIAS` column is computed, so both are accepted, and so are a subcolumn of a column of
-- the structure and a virtual column of the function's storage, such as `_path` or a hive partition column.
-- https://github.com/ClickHouse/ClickHouse/issues/129062

SELECT '--- a column the explicit structure does not have';
CREATE TABLE t_random (x String) AS generateRandom('a UInt8'); -- { serverError THERE_IS_NO_COLUMN }
CREATE TABLE t_null (x String) AS null('a UInt8'); -- { serverError THERE_IS_NO_COLUMN }
CREATE TABLE t_values (x String) AS values('a UInt8', 1); -- { serverError THERE_IS_NO_COLUMN }
-- The file is not read, so it does not need to exist.
CREATE TABLE t_file (x String) AS file('05567_missing.csv', 'CSV', 'a UInt8, b String'); -- { serverError THERE_IS_NO_COLUMN }

SELECT '--- a column a function with a fixed structure does not have';
CREATE TABLE t_numbers (y UInt64) AS numbers(3); -- { serverError THERE_IS_NO_COLUMN }

SELECT '--- a DEFAULT or MATERIALIZED column is read from the function too';
CREATE TABLE t_default (number UInt64, d UInt64 DEFAULT 7) AS numbers(3); -- { serverError THERE_IS_NO_COLUMN }
CREATE TABLE t_materialized (number UInt64, m UInt64 MATERIALIZED number * 2) AS numbers(3); -- { serverError THERE_IS_NO_COLUMN }

SELECT '--- an ALIAS column is computed';
CREATE TABLE t_alias (number UInt64, a UInt64 ALIAS number * 2) AS numbers(3);
SELECT number, a FROM t_alias ORDER BY number;

SELECT '--- the same name with another type is converted';
CREATE TABLE t_type (a UInt16) AS generateRandom('a UInt8', 1);
SELECT toTypeName(a) FROM t_type LIMIT 1;

SELECT '--- a subset of the columns of the structure';
CREATE TABLE t_subset (b String) AS values('a UInt8, b String', (1, 'x'));
SELECT b FROM t_subset;

SELECT '--- a subcolumn of a column of the structure';
CREATE TABLE t_subcolumn (`t.x` UInt8) AS values('t Tuple(x UInt8)', tuple(7));
SELECT `t.x` FROM t_subcolumn;

SELECT '--- a virtual column and a hive partition column of the storage';
-- The files are named after the database, so that concurrent runs of the test do not share them.
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_05567_hive/k=v1/f.csv', 'CSV', 'a UInt8') SELECT * FROM values('a UInt8', 1, 2)
SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_05567_hive/k=v2/f.csv', 'CSV', 'a UInt8') SELECT * FROM values('a UInt8', 3)
SETTINGS engine_file_truncate_on_insert = 1;
CREATE TABLE t_virtual (a UInt8, _path String, _file String) AS file(currentDatabase() || '_05567_hive/*/f.csv', 'CSV', 'a UInt8');
SELECT a, _file, endsWith(_path, '/f.csv') FROM t_virtual ORDER BY a;
-- `use_hive_partitioning` is enabled by default.
CREATE TABLE t_hive (a UInt8, k String) AS file(currentDatabase() || '_05567_hive/*/f.csv', 'CSV', 'a UInt8');
SELECT a, k FROM t_hive ORDER BY a;
CREATE TABLE t_not_virtual (a UInt8, _nothing String) AS file(currentDatabase() || '_05567_hive/*/f.csv', 'CSV', 'a UInt8'); -- { serverError THERE_IS_NO_COLUMN }
-- The hive partition columns come from the files that match the path at `CREATE`, so without such files `k` is rejected.
CREATE TABLE t_hive_no_files (a UInt8, k String) AS file(currentDatabase() || '_05567_no_files/*/f.csv', 'CSV', 'a UInt8'); -- { serverError THERE_IS_NO_COLUMN }

SELECT '--- a function whose storage takes the columns of the table';
SET enable_elasticsearch = 1;
CREATE TABLE t_elasticsearch (payload JSON) AS elasticsearch('http://localhost:1', 'idx');
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_elasticsearch';

SELECT '--- a function that takes the columns of the table as its structure is not checked';
CREATE TABLE t_hint (x String) AS null();
SELECT count() FROM t_hint;

DROP TABLE t_alias;
DROP TABLE t_type;
DROP TABLE t_subset;
DROP TABLE t_hint;
DROP TABLE t_subcolumn;
DROP TABLE t_virtual;
DROP TABLE t_hive;
DROP TABLE t_elasticsearch;
