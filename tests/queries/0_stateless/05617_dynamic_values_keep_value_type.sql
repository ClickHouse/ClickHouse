-- A `Date`/`DateTime`/`Enum` inside a `Dynamic` constant must keep its type in `INSERT ... VALUES`
-- and in the `values` table function, instead of being stored as the underlying integer.
-- https://github.com/ClickHouse/ClickHouse/issues/130088

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_dynamic_values_map;
CREATE TABLE t_dynamic_values_map (id UInt8, m Map(String, Dynamic)) ENGINE = Memory;
INSERT INTO t_dynamic_values_map VALUES (1, map('d', CAST(toDate('2020-01-01') AS Dynamic)));
INSERT INTO t_dynamic_values_map VALUES (2, map('dt', CAST(toDateTime('2020-01-01 10:20:30') AS Dynamic)));
INSERT INTO t_dynamic_values_map VALUES (3, map('e', CAST(CAST('b', 'Enum8(\'a\' = 1, \'b\' = 2)') AS Dynamic)));
INSERT INTO t_dynamic_values_map VALUES (4, map('b', CAST(true AS Dynamic)));
INSERT INTO t_dynamic_values_map VALUES (5, map('n', CAST(NULL AS Dynamic)));
INSERT INTO t_dynamic_values_map VALUES (6, map('d', toDate('2020-01-01')));
INSERT INTO t_dynamic_values_map SETTINGS input_format_values_deduce_templates_of_expressions = 0 VALUES (7, map('d', CAST(toDate('2020-01-01') AS Dynamic)));
SELECT id, arrayMap(x -> (x.1, toString(x.2), dynamicType(x.2)), CAST(m, 'Array(Tuple(String, Dynamic))')) FROM t_dynamic_values_map ORDER BY id;
DROP TABLE t_dynamic_values_map;

DROP TABLE IF EXISTS t_dynamic_values;
CREATE TABLE t_dynamic_values (id UInt8, d Dynamic) ENGINE = Memory;
INSERT INTO t_dynamic_values VALUES (1, CAST(toDate('2020-01-01') AS Dynamic));
INSERT INTO t_dynamic_values SETTINGS input_format_values_deduce_templates_of_expressions = 0 VALUES (2, CAST(toDateTime('2020-01-01 10:20:30') AS Dynamic));
INSERT INTO t_dynamic_values SETTINGS input_format_values_deduce_templates_of_expressions = 0 VALUES (3, toDate('2020-01-01'));
SELECT id, toString(d), dynamicType(d) FROM t_dynamic_values ORDER BY id;
DROP TABLE t_dynamic_values;

-- `input_format_null_as_default` still replaces a `NULL` nested in a value with dynamic structure.
DROP TABLE IF EXISTS t_dynamic_values_null;
CREATE TABLE t_dynamic_values_null (t Tuple(a Int32, b Dynamic)) ENGINE = Memory;
INSERT INTO t_dynamic_values_null SETTINGS input_format_null_as_default = 1, input_format_values_deduce_templates_of_expressions = 0 VALUES ((CAST(NULL AS Nullable(Int32)), CAST('x' AS Dynamic)));
SELECT t.a, toString(t.b), dynamicType(t.b) FROM t_dynamic_values_null;
DROP TABLE t_dynamic_values_null;

SELECT toString(d), dynamicType(d) FROM values('d Dynamic', CAST(toDate('2020-01-01') AS Dynamic));
SELECT toString(d), dynamicType(d) FROM values('d Dynamic', CAST(toDateTime('2020-01-01 10:20:30') AS Dynamic));
SELECT toString(d), dynamicType(d) FROM values('d Dynamic', CAST(CAST('b', 'Enum8(\'a\' = 1, \'b\' = 2)') AS Dynamic));
SELECT toString(d), dynamicType(d) FROM values('d Dynamic', toDate('2020-01-01'));
SELECT toString(d), dynamicType(d) FROM values('d Dynamic', CAST(true AS Dynamic));
SELECT toString(d), dynamicType(d) FROM values('d Dynamic', 42);
SELECT toString(d), dynamicType(d) FROM values('d Dynamic', 'abc');
SELECT toString(d), dynamicType(d) FROM values('d Dynamic', CAST(NULL AS Dynamic));
SELECT id, toString(d), dynamicType(d) FROM values('id UInt8, d Dynamic', (1, CAST(toDate('2020-01-01') AS Dynamic)), (2, toDateTime('2020-01-01 10:20:30')));

-- `Dynamic` nested in other types and `JSON` in the `values` table function.
SET enable_json_type = 1;
SELECT toString(m), dynamicType(m['d']) FROM values('m Map(String, Dynamic)', map('d', CAST(toDate('2020-01-01') AS Dynamic)));
SELECT toString(m), dynamicType(m['d']) FROM values('m Map(String, Dynamic)', map('d', toDate('2020-01-01')));
SELECT toString(a), dynamicType(a[1]) FROM values('a Array(Dynamic)', [CAST(toDateTime('2020-01-01 10:20:30') AS Dynamic)]);
SELECT toString(t), dynamicType(t.1) FROM values('t Tuple(Dynamic)', tuple(CAST(CAST('b', 'Enum8(\'a\' = 1, \'b\' = 2)') AS Dynamic)));
SELECT toString(j), dynamicType(j.d), j.d.:Date FROM values('j JSON', CAST(map('d', CAST(toDate('2020-01-01') AS Dynamic)) AS JSON));

-- `input_format_null_as_default` replaces a `NULL` nested in a value with dynamic structure and keeps the types of the other values.
DROP TABLE IF EXISTS t_dynamic_values_null_date;
CREATE TABLE t_dynamic_values_null_date (id UInt8, t Tuple(a Int32, b Dynamic), m Map(String, Tuple(a Array(Int32), b Dynamic))) ENGINE = Memory;
INSERT INTO t_dynamic_values_null_date SETTINGS input_format_null_as_default = 1, input_format_values_deduce_templates_of_expressions = 0 VALUES
    (1, (CAST(NULL AS Nullable(Int32)), CAST(toDate('2020-01-01') AS Dynamic)), map('k', (CAST([1, NULL] AS Array(Nullable(Int32))), CAST(toDateTime('2020-01-01 10:20:30') AS Dynamic))));
INSERT INTO t_dynamic_values_null_date SETTINGS input_format_null_as_default = 1, input_format_values_deduce_templates_of_expressions = 0 VALUES
    (2, (CAST(5 AS Nullable(Int32)), CAST(toDate('2020-01-01') AS Dynamic)), map('k', (CAST([1] AS Array(Nullable(Int32))), CAST(toDate('2020-01-01') AS Dynamic))));
SELECT id, t.a, toString(t.b), dynamicType(t.b), toString(m['k'].a), toString(m['k'].b), dynamicType(m['k'].b) FROM t_dynamic_values_null_date ORDER BY id;
DROP TABLE t_dynamic_values_null_date;

-- A string is still parsed as text of the destination type.
SELECT toString(m) FROM values('m Map(String, JSON)', '{\'x\' : \'{"y" : 1}\'}');
