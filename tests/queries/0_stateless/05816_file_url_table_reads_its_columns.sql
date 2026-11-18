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
DROP TABLE t_url;
