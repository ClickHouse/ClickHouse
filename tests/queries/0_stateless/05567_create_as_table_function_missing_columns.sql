-- A table created `AS` a table function reads the columns of the function's storage by name, so `CREATE` rejects a
-- stored column of the table that the structure of the function does not have: such a table could never be read.
-- Another type for the same name is converted, and an `ALIAS` column is computed, so both are accepted.
-- https://github.com/ClickHouse/ClickHouse/issues/129062

SELECT '--- a column the explicit structure does not have';
CREATE TABLE t_random (x String) AS generateRandom('a UInt8'); -- { serverError THERE_IS_NO_COLUMN }
CREATE TABLE t_null (x String) AS null('a UInt8'); -- { serverError THERE_IS_NO_COLUMN }
CREATE TABLE t_values (x String) AS values('a UInt8', 1); -- { serverError THERE_IS_NO_COLUMN }
-- Rejected before the file is opened, so the file does not need to exist.
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

SELECT '--- a function that takes the columns of the table as its structure is not checked';
CREATE TABLE t_hint (x String) AS null();
SELECT count() FROM t_hint;

DROP TABLE t_alias;
DROP TABLE t_type;
DROP TABLE t_subset;
DROP TABLE t_hint;
