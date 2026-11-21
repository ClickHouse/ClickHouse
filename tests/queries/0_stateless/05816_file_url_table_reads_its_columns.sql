-- A table created `AS file(...)` or `AS url(...)` without a structure argument could not be read if its columns, given in
-- `CREATE`, were not among the columns inferred from the data, such as `c1` and `c2` of a CSV file. Such a table now reads its
-- own columns, while one with some of the inferred columns still reads them by name.

INSERT INTO FUNCTION file(currentDatabase() || '_05816.csv', 'CSV', 'id UInt32, s String') SELECT number + 1, ['hello', 'world'][number + 1] FROM numbers(2);

SELECT '--- given columns';
CREATE TABLE t_given (id UInt32, s String) AS file(currentDatabase() || '_05816.csv', 'CSV');
SELECT * FROM t_given ORDER BY id;
SELECT id, s, _file LIKE '%_05816.csv' FROM t_given ORDER BY id;
DETACH TABLE t_given;
ATTACH TABLE t_given;
SELECT * FROM t_given ORDER BY id;

SELECT '--- given columns and a virtual column';
CREATE TABLE t_given_virtual (id UInt32, s String, _file String) AS file(currentDatabase() || '_05816.csv', 'CSV');
SELECT id, s, _file LIKE '%_05816.csv' FROM t_given_virtual ORDER BY id;

SELECT '--- inferred columns in another order';
CREATE TABLE t_reordered (c2 String, c1 UInt32) AS file(currentDatabase() || '_05816.csv', 'CSV');
SELECT * FROM t_reordered ORDER BY c1;

SELECT '--- inferred columns and another one';
CREATE TABLE t_partial (c1 UInt32, c2 String, extra String) AS file(currentDatabase() || '_05816.csv', 'CSV');
SELECT c1, c2 FROM t_partial ORDER BY c1;

SELECT '--- a materialized column among the inferred columns';
CREATE TABLE t_materialized (id UInt32, s String, c1 UInt32 MATERIALIZED id + 10) AS file(currentDatabase() || '_05816.csv', 'CSV');
SELECT c1 FROM t_materialized ORDER BY c1;

SELECT '--- a subcolumn among the inferred columns';
INSERT INTO FUNCTION file(currentDatabase() || '_05816.jsonl', 'JSONEachRow', '`nested.value` UInt32, other String') VALUES (42, 'hello');
CREATE TABLE t_subcolumn (nested Tuple(value UInt32), renamed String) AS file(currentDatabase() || '_05816.jsonl', 'JSONEachRow');
SELECT nested.value FROM t_subcolumn;

SELECT '--- a dynamic subcolumn among the inferred columns';
INSERT INTO FUNCTION file(currentDatabase() || '_05816_json.jsonl', 'JSONEachRow', '`j.a` UInt32, other String') VALUES (7, 'hello');
CREATE TABLE t_dynamic_subcolumn (j JSON, renamed String) AS file(currentDatabase() || '_05816_json.jsonl', 'JSONEachRow');
SELECT j.a FROM t_dynamic_subcolumn;

SELECT '--- fewer given columns than a format that cannot read a subset of its columns has';
CREATE TABLE t_fewer (id UInt32) AS file(currentDatabase() || '_05816.csv', 'CSV');
SELECT _row_number, _file LIKE '%_05816.csv' FROM t_fewer ORDER BY _row_number SETTINGS optimize_count_from_files = 0;

SELECT '--- fewer given columns than a format that can read a subset of its columns has';
CREATE TABLE t_fewer_subset (renamed String) AS file(currentDatabase() || '_05816.jsonl', 'JSONEachRow');
SELECT renamed = '', _row_number FROM t_fewer_subset;

SELECT '--- given columns of a file in a hive-style partition, which the inferred columns also have';
SET use_hive_partitioning = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05816_hive/key=7/data.csv', 'CSV', 'id UInt32, s String') SELECT number + 1, ['hello', 'world'][number + 1] FROM numbers(2);
CREATE TABLE t_hive (id UInt32, s String) AS file(currentDatabase() || '_05816_hive/key=7/data.csv', 'CSV');
SELECT * FROM t_hive ORDER BY id;

SELECT '--- given columns and the partition column of a file in a hive-style partition';
CREATE TABLE t_hive_key (id UInt32, s String, key UInt32) AS file(currentDatabase() || '_05816_hive/key=7/data.csv', 'CSV');
SELECT * FROM t_hive_key ORDER BY id;
SELECT key FROM t_hive_key;

SELECT '--- fewer given columns than a format detected from the data, which can read a subset of its columns, has';
INSERT INTO FUNCTION file(currentDatabase() || '_05816_jsonl_without_extension', 'JSONEachRow', 'a UInt32, b String') VALUES (42, 'hello');
CREATE TABLE t_detected (renamed String) AS file(currentDatabase() || '_05816_jsonl_without_extension');
SELECT renamed = '' FROM t_detected;

SELECT '--- given columns with defaults that use the given columns';
INSERT INTO FUNCTION file(currentDatabase() || '_05816_external.jsonl', 'JSONEachRow', 'external UInt32') VALUES (1);
CREATE TABLE t_defaults (id UInt32 DEFAULT 5, next UInt32 DEFAULT id + 1) AS file(currentDatabase() || '_05816_external.jsonl', 'JSONEachRow');
SELECT id, next FROM t_defaults;

SELECT '--- a default that uses an alias column, which the storage cannot have';
CREATE TABLE t_default_alias (id UInt32 DEFAULT helper, helper UInt32 ALIAS 42) AS file(currentDatabase() || '_05816_external.jsonl', 'JSONEachRow');
SELECT id FROM t_default_alias; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

SELECT '--- url';
CREATE TABLE t_url (x UInt8, y String) AS url('http://127.0.0.1:8123/?query=SELECT+1,%27a%27+FORMAT+CSV', 'CSV');
SELECT * FROM t_url;
DETACH TABLE t_url;
ATTACH TABLE t_url;
SELECT * FROM t_url;

DROP TABLE t_given;
DROP TABLE t_given_virtual;
DROP TABLE t_reordered;
DROP TABLE t_partial;
DROP TABLE t_materialized;
DROP TABLE t_subcolumn;
DROP TABLE t_dynamic_subcolumn;
DROP TABLE t_fewer;
DROP TABLE t_fewer_subset;
DROP TABLE t_hive;
DROP TABLE t_hive_key;
DROP TABLE t_detected;
DROP TABLE t_defaults;
DROP TABLE t_default_alias;
DROP TABLE t_url;
