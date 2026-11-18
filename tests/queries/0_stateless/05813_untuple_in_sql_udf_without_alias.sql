-- Tags: no-parallel
-- ^ creates global SQL UDFs (CREATE FUNCTION); concurrent copies of this test would clash on the names.

DROP FUNCTION IF EXISTS untuple_value_05813;
DROP FUNCTION IF EXISTS untuple_value_nested_05813;
DROP FUNCTION IF EXISTS format_row_05813;
DROP FUNCTION IF EXISTS untuple_map_05813;
DROP FUNCTION IF EXISTS map_05813;
DROP FUNCTION IF EXISTS apply_lambda_05813;
DROP FUNCTION IF EXISTS wrap_apply_lambda_05813;

CREATE FUNCTION untuple_value_05813 AS (x) -> untuple(tuple(x));
CREATE FUNCTION untuple_value_nested_05813 AS (y) -> untuple_value_05813(y);
CREATE FUNCTION format_row_05813 AS (x) -> formatRowNoNewline('JSONEachRow', x);
CREATE FUNCTION untuple_map_05813 AS (fn, xs) -> untuple(tuple(arrayMap(fn, xs)));
CREATE FUNCTION map_05813 AS (fn, xs) -> arrayMap(fn, xs);
CREATE FUNCTION apply_lambda_05813 AS (fn, x) -> fn(x);
CREATE FUNCTION wrap_apply_lambda_05813 AS (x) -> apply_lambda_05813(y -> untuple(tuple(y)), x);

SELECT '-- https://github.com/ClickHouse/ClickHouse/issues/131545';
SELECT * FROM (SELECT untuple_value_05813(11), untuple_value_05813(22)) FORMAT TSVWithNames;
SELECT '-- the inner query alone';
SELECT untuple_value_05813(11), untuple_value_05813(22) FORMAT TSVWithNames;
SELECT '-- column arguments';
SELECT * FROM (SELECT untuple_value_05813(number), untuple_value_05813(number + 1) FROM numbers(2)) ORDER BY 1 FORMAT TSVWithNames;
SELECT '-- nested function';
SELECT * FROM (SELECT untuple_value_nested_05813(11), untuple_value_nested_05813(22)) FORMAT TSVWithNames;
SELECT '-- lambda arguments';
SELECT * FROM (SELECT untuple_map_05813(x -> x + 1, [1]), untuple_map_05813(x -> x + 2, [1])) FORMAT TSVWithNames;
SELECT * FROM (SELECT map_05813(x -> x + number, [1]), map_05813(x -> x * number, [1]) FROM numbers(2)) ORDER BY 1 FORMAT TSVWithNames;
SELECT * FROM (SELECT untuple_map_05813(x -> toTypeName(tuple(x AS a)), [1]), untuple_map_05813(x -> toTypeName(tuple(x AS b)), [1])) FORMAT TSVWithNames
SETTINGS enable_named_columns_in_function_tuple = 1, print_pretty_type_names = 0;
SELECT '-- a lambda argument called in the body';
SELECT * FROM (SELECT apply_lambda_05813(y -> untuple(tuple(y)), 11), apply_lambda_05813(y -> untuple(tuple(y)), 22)) FORMAT TSVWithNames;
SELECT * FROM (SELECT wrap_apply_lambda_05813(11), wrap_apply_lambda_05813(22)) FORMAT TSVWithNames;
SELECT '-- a SQL UDF called in a WITH lambda: the WITH call names the columns';
SELECT * FROM (WITH x -> untuple_value_05813(x) AS l SELECT l(11), l(22)) FORMAT TSVWithNames;
WITH x -> untuple_value_05813(x) AS l SELECT l(11), l(22) FORMAT TSVWithNames;
SELECT * FROM (WITH (fn, v) -> untuple_value_05813(fn(v)) AS l SELECT l(x -> x + 1, 1), l(x -> x + 2, 1)) FORMAT TSVWithNames;
SELECT '-- WITH lambdas without a SQL UDF call keep their names: stored view definitions keep WITH lambdas';
SELECT * FROM (WITH x -> untuple(tuple(x)) AS l SELECT l(11)) FORMAT TSVWithNames;
SELECT * FROM (WITH x -> untuple(tuple(x)) AS l, y -> l(y) AS k SELECT k(11)) FORMAT TSVWithNames;
SELECT '-- an alias still names the columns';
SELECT * FROM (SELECT untuple_value_05813(11) AS a, untuple_value_05813(22) AS b) FORMAT TSVWithNames;
SELECT '-- views: CREATE substitutes SQL UDFs';
CREATE VIEW v AS SELECT untuple_value_05813(11), untuple_value_05813(22);
SELECT * FROM v FORMAT TSVWithNames;
CREATE VIEW v_with AS WITH x -> untuple_value_05813(x) AS l SELECT l(11);
SELECT * FROM v_with FORMAT TSVWithNames;
SELECT '-- remote()';
SELECT * FROM (SELECT untuple_value_05813(dummy + 11), untuple_value_05813(dummy + 22) FROM remote('127.0.0.1', system.one)) FORMAT TSVWithNames;
SELECT '-- names inside the body keep the parameter name';
WITH x -> tupleElement(tuple(x), 'x') AS t SELECT t(1) SETTINGS enable_named_columns_in_function_tuple = 1;
SELECT format_row_05813(number) FROM numbers(1);

DROP VIEW v;
DROP VIEW v_with;
DROP FUNCTION untuple_value_nested_05813;
DROP FUNCTION untuple_value_05813;
DROP FUNCTION format_row_05813;
DROP FUNCTION untuple_map_05813;
DROP FUNCTION map_05813;
DROP FUNCTION wrap_apply_lambda_05813;
DROP FUNCTION apply_lambda_05813;
