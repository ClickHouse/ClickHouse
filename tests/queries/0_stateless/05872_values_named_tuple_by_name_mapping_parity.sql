-- The interpreted `VALUES` path (`input_format_values_deduce_templates_of_expressions = 0`) must store the same
-- value as the expression template path and as `INSERT ... SELECT`: named tuples are matched by name with the
-- mapping of `CAST`, including partially overlapping names, missing destination elements, and tuples nested in
-- `Array`, `Map` and `Nullable`. The insert guard applies to the nested tuples in the same way.

DROP TABLE IF EXISTS t_values_named_tuple;
CREATE TABLE t_values_named_tuple (x Tuple(a Int32, b Int32)) ENGINE = Memory;
SELECT 'partial overlap, guard disabled';
INSERT INTO t_values_named_tuple SETTINGS allow_named_tuple_conversion_with_extra_source_fields_on_insert = 1, input_format_values_deduce_templates_of_expressions = 1 VALUES (tuple('b2', 'a')(1, 2));
INSERT INTO t_values_named_tuple SETTINGS allow_named_tuple_conversion_with_extra_source_fields_on_insert = 1, input_format_values_deduce_templates_of_expressions = 0 VALUES (tuple('b2', 'a')(1, 2));
INSERT INTO t_values_named_tuple SETTINGS allow_named_tuple_conversion_with_extra_source_fields_on_insert = 1 SELECT tuple('b2', 'a')(1, 2);
SELECT * FROM t_values_named_tuple;
TRUNCATE TABLE t_values_named_tuple;
SELECT 'partial overlap, guard enabled';
INSERT INTO t_values_named_tuple SETTINGS input_format_values_deduce_templates_of_expressions = 1 VALUES (tuple('b2', 'a')(1, 2)); -- { error CANNOT_CONVERT_TYPE }
INSERT INTO t_values_named_tuple SETTINGS input_format_values_deduce_templates_of_expressions = 0 VALUES (tuple('b2', 'a')(1, 2)); -- { error CANNOT_CONVERT_TYPE }
INSERT INTO t_values_named_tuple SELECT tuple('b2', 'a')(1, 2); -- { error CANNOT_CONVERT_TYPE }
SELECT count() FROM t_values_named_tuple;
DROP TABLE t_values_named_tuple;
CREATE TABLE t_values_named_tuple (x Tuple(a Int32, b Int32, c Int32)) ENGINE = Memory;
SELECT 'schema expansion';
INSERT INTO t_values_named_tuple SETTINGS input_format_values_deduce_templates_of_expressions = 1 VALUES (tuple('b', 'a')(1, 2));
INSERT INTO t_values_named_tuple SETTINGS input_format_values_deduce_templates_of_expressions = 0 VALUES (tuple('b', 'a')(1, 2));
INSERT INTO t_values_named_tuple SELECT tuple('b', 'a')(1, 2);
SELECT * FROM t_values_named_tuple;
DROP TABLE t_values_named_tuple;
CREATE TABLE t_values_named_tuple (x Array(Tuple(a Int32, b Int32)), m Map(String, Tuple(a Int32, b Int32)), n Nullable(Tuple(a Int32, b Int32))) ENGINE = Memory;
SELECT 'containers';
INSERT INTO t_values_named_tuple SETTINGS input_format_values_deduce_templates_of_expressions = 1 VALUES ([tuple('b', 'a')(1, 2)], map('k', tuple('b', 'a')(3, 4)), tuple('b', 'a')(5, 6));
INSERT INTO t_values_named_tuple SETTINGS input_format_values_deduce_templates_of_expressions = 0 VALUES ([tuple('b', 'a')(1, 2)], map('k', tuple('b', 'a')(3, 4)), tuple('b', 'a')(5, 6));
INSERT INTO t_values_named_tuple SELECT [tuple('b', 'a')(1, 2)], map('k', tuple('b', 'a')(3, 4)), tuple('b', 'a')(5, 6);
SELECT * FROM t_values_named_tuple;
TRUNCATE TABLE t_values_named_tuple;
SELECT 'containers, guard';
INSERT INTO t_values_named_tuple SETTINGS input_format_values_deduce_templates_of_expressions = 1 VALUES ([tuple('b2', 'a')(1, 2)], map(), NULL); -- { error CANNOT_CONVERT_TYPE }
INSERT INTO t_values_named_tuple SETTINGS input_format_values_deduce_templates_of_expressions = 0 VALUES ([tuple('b2', 'a')(1, 2)], map(), NULL); -- { error CANNOT_CONVERT_TYPE }
INSERT INTO t_values_named_tuple SELECT [tuple('b2', 'a')(1, 2)], map(), NULL; -- { error CANNOT_CONVERT_TYPE }
INSERT INTO t_values_named_tuple SETTINGS input_format_values_deduce_templates_of_expressions = 1 VALUES ([], map('k', tuple('b2', 'a')(3, 4)), NULL); -- { error CANNOT_CONVERT_TYPE }
INSERT INTO t_values_named_tuple SETTINGS input_format_values_deduce_templates_of_expressions = 0 VALUES ([], map('k', tuple('b2', 'a')(3, 4)), NULL); -- { error CANNOT_CONVERT_TYPE }
SELECT count() FROM t_values_named_tuple;
SELECT 'containers, guard disabled';
INSERT INTO t_values_named_tuple SETTINGS allow_named_tuple_conversion_with_extra_source_fields_on_insert = 1, input_format_values_deduce_templates_of_expressions = 1 VALUES ([tuple('b2', 'a')(1, 2)], map('k', tuple('b2', 'a')(3, 4)), tuple('b2', 'a')(5, 6));
INSERT INTO t_values_named_tuple SETTINGS allow_named_tuple_conversion_with_extra_source_fields_on_insert = 1, input_format_values_deduce_templates_of_expressions = 0 VALUES ([tuple('b2', 'a')(1, 2)], map('k', tuple('b2', 'a')(3, 4)), tuple('b2', 'a')(5, 6));
INSERT INTO t_values_named_tuple SETTINGS allow_named_tuple_conversion_with_extra_source_fields_on_insert = 1 SELECT [tuple('b2', 'a')(1, 2)], map('k', tuple('b2', 'a')(3, 4)), tuple('b2', 'a')(5, 6);
SELECT * FROM t_values_named_tuple;
DROP TABLE t_values_named_tuple;
