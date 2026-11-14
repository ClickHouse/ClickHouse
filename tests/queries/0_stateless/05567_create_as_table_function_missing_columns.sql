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
CREATE TABLE t_format (x String) AS format('CSV', 'a UInt8', '7\n'); -- { serverError THERE_IS_NO_COLUMN }
-- The missing column is reported without parsing the data.
CREATE TABLE t_format_unparsed_missing (x String) AS format('CSV', 'a UInt8', 'abc\n'); -- { serverError THERE_IS_NO_COLUMN }

SELECT '--- an ALIAS or EPHEMERAL column of the structure, which the storage of the function does not compute';
CREATE TABLE t_structure_alias (x UInt8) AS values('a UInt8, x UInt8 ALIAS a', 7); -- { serverError THERE_IS_NO_COLUMN }
CREATE TABLE t_structure_ephemeral (x UInt8) AS values('a UInt8, x UInt8 EPHEMERAL', 7); -- { serverError THERE_IS_NO_COLUMN }
CREATE TABLE t_structure_random (x UInt8) AS generateRandom('a UInt8, x UInt8 ALIAS a', 1); -- { serverError THERE_IS_NO_COLUMN }

SELECT '--- a subcolumn of the structure, which the storage of generateRandom does not read by its name';
CREATE TABLE t_subcolumn_random (`t.x` UInt8) AS generateRandom('t Tuple(x UInt8)', 1); -- { serverError THERE_IS_NO_COLUMN }

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

SELECT '--- the column of format with an explicit structure';
CREATE TABLE t_format_column (a UInt8) AS format('CSV', 'a UInt8', '7\n');
SELECT a FROM t_format_column;
-- A MATERIALIZED column of the structure, which `format` reads like the others.
CREATE TABLE t_format_materialized (x UInt8) AS format('CSV', 'a UInt8, x UInt8 MATERIALIZED a', '7,9\n');
SELECT x FROM t_format_materialized;
-- The data is parsed when the table is first read, not when it is created or loaded.
CREATE TABLE t_format_unparsed (a UInt8) AS format('CSV', 'a UInt8', 'abc\n');
SELECT a FROM t_format_unparsed; -- { serverError INCORRECT_DATA }

SELECT '--- a subset of the columns of the structure';
CREATE TABLE t_subset (b String) AS values('a UInt8, b String', (1, 'x'));
SELECT b FROM t_subset;

SELECT '--- a subcolumn of a column of the structure';
CREATE TABLE t_subcolumn (`t.x` UInt8) AS values('t Tuple(x UInt8)', tuple(7));
SELECT `t.x` FROM t_subcolumn;
-- The storage of `file` reads subcolumns, which is known without building it, so the file does not need to exist.
CREATE TABLE t_file_subcolumn (`t.x` UInt8) AS file('05567_missing.csv', 'CSV', 't Tuple(x UInt8)');

SELECT '--- a virtual column and a hive partition column of the storage';
-- The files are named after the database, so that concurrent runs of the test do not share them.
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_05567_hive/k=v1/f.csv', 'CSV', 'a UInt8') SELECT * FROM values('a UInt8', 1, 2)
SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_05567_hive/k=v2/f.csv', 'CSV', 'a UInt8') SELECT * FROM values('a UInt8', 3)
SETTINGS engine_file_truncate_on_insert = 1;
CREATE TABLE t_virtual (a UInt8, _path String, _file String) AS file(currentDatabase() || '_05567_hive/*/f.csv', 'CSV', 'a UInt8');
SELECT a, _file, endsWith(_path, '/f.csv') FROM t_virtual ORDER BY a;
-- `use_hive_partitioning` is enabled by default. A path with globs may match a hive partition column of any name, so the
-- name is accepted without depending on the files that exist now, also if no file matches the path yet.
CREATE TABLE t_hive (a UInt8, k String) AS file(currentDatabase() || '_05567_hive/*/f.csv', 'CSV', 'a UInt8');
SELECT a, k FROM t_hive ORDER BY a;
CREATE TABLE t_hive_no_files (a UInt8, k String) AS file(currentDatabase() || '_05567_no_files/*/f.csv', 'CSV', 'a UInt8');
-- Without globs, the hive partition columns are those of the path.
CREATE TABLE t_hive_path (a UInt8, k String) AS file(currentDatabase() || '_05567_hive/k=v1/f.csv', 'CSV', 'a UInt8');
SELECT a, k FROM t_hive_path ORDER BY a;
CREATE TABLE t_not_hive (a UInt8, j String) AS file(currentDatabase() || '_05567_hive/k=v1/f.csv', 'CSV', 'a UInt8'); -- { serverError THERE_IS_NO_COLUMN }
CREATE TABLE t_not_virtual (a UInt8, _nothing String) AS file(currentDatabase() || '_05567_hive/k=v1/f.csv', 'CSV', 'a UInt8'); -- { serverError THERE_IS_NO_COLUMN }
-- A directory lists its files, which take the hive partition columns of the directory, also before it has any: the file
-- here is in a subdirectory, which is not listed.
INSERT INTO TABLE FUNCTION file(currentDatabase() || '_05567_directory/k=v/sub/f.csv', 'CSV', 'a UInt8') SELECT 1
SETTINGS engine_file_truncate_on_insert = 1;
CREATE TABLE t_directory (a UInt8, k String) AS file(currentDatabase() || '_05567_directory/k=v', 'CSV', 'a UInt8');
CREATE TABLE t_directory_not_hive (a UInt8, j String) AS file(currentDatabase() || '_05567_directory/k=v', 'CSV', 'a UInt8'); -- { serverError THERE_IS_NO_COLUMN }
CREATE TABLE t_directory_files (a UInt8, k String) AS file(currentDatabase() || '_05567_hive/k=v1', 'CSV', 'a UInt8');
SELECT a, k FROM t_directory_files ORDER BY a;
-- The first path of an array is its first file, unless it is a glob or a directory: then which of the paths has the first
-- file depends on the files that exist, so any name is accepted, as for a glob.
CREATE TABLE t_array_files (a UInt8, k String) AS file([currentDatabase() || '_05567_hive/k=v1/f.csv', currentDatabase() || '_05567_hive/k=v2/f.csv'], 'CSV', 'a UInt8');
SELECT a, k FROM t_array_files ORDER BY a;
CREATE TABLE t_array_files_not_hive (a UInt8, j String) AS file([currentDatabase() || '_05567_hive/k=v1/f.csv', currentDatabase() || '_05567_hive/k=v2/f.csv'], 'CSV', 'a UInt8'); -- { serverError THERE_IS_NO_COLUMN }
CREATE TABLE t_array_missing_file_not_hive (a UInt8, j String) AS file([currentDatabase() || '_05567_hive/k=v1/missing.csv', currentDatabase() || '_05567_hive/k=v2/f.csv'], 'CSV', 'a UInt8'); -- { serverError THERE_IS_NO_COLUMN }
CREATE TABLE t_array (a UInt8, k String) AS file([currentDatabase() || '_05567_directory/k=v', currentDatabase() || '_05567_hive/k=v1'], 'CSV', 'a UInt8');
CREATE TABLE t_array_any (a UInt8, j String) AS file([currentDatabase() || '_05567_directory/k=v', currentDatabase() || '_05567_hive/k=v1'], 'CSV', 'a UInt8');
-- A file in an archive has no hive partition columns.
CREATE TABLE t_archive (a UInt8, k String) AS file(currentDatabase() || '_05567_hive/k=v1/a.zip :: f.csv', 'CSV', 'a UInt8'); -- { serverError THERE_IS_NO_COLUMN }
-- A path with `{_partition_id}` is not listed, so it is not a glob: its hive partition columns are those of the path.
CREATE TABLE t_partition_id (a UInt8, j String) AS file(currentDatabase() || '_05567_hive/k=v1/{_partition_id}.csv', 'CSV', 'a UInt8'); -- { serverError THERE_IS_NO_COLUMN }

SELECT '--- a function whose storage takes the columns of the table';
SET enable_elasticsearch = 1;
CREATE TABLE t_elasticsearch (payload JSON) AS elasticsearch('http://localhost:1', 'idx');
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_elasticsearch';
-- The storage does not read subcolumns, but `document.foo`, named like a subcolumn of the default structure `document JSON`,
-- is a column of the table, so an ordinary column of the storage.
CREATE TABLE t_elasticsearch_dotted (`document.foo` JSON) AS elasticsearch('http://localhost:1', 'idx');
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_elasticsearch_dotted';

SELECT '--- a function that takes the columns of the table as its structure is not checked';
CREATE TABLE t_hint (x String) AS null();
SELECT count() FROM t_hint;

DROP TABLE t_alias;
DROP TABLE t_type;
DROP TABLE t_subset;
DROP TABLE t_format_column;
DROP TABLE t_format_materialized;
DROP TABLE t_format_unparsed;
DROP TABLE t_hint;
DROP TABLE t_subcolumn;
DROP TABLE t_file_subcolumn;
DROP TABLE t_virtual;
DROP TABLE t_hive;
DROP TABLE t_directory;
DROP TABLE t_directory_files;
DROP TABLE t_array_files;
DROP TABLE t_array;
DROP TABLE t_array_any;
DROP TABLE t_hive_no_files;
DROP TABLE t_hive_path;
DROP TABLE t_elasticsearch;
DROP TABLE t_elasticsearch_dotted;
