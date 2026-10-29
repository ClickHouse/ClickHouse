-- Column matchers in default expressions inside lambdas, nested `APPLY` and `REPLACE`.

SELECT '-- a matched column must not be bound by an enclosing lambda parameter';
CREATE TABLE t_capture (a UInt8, b Array(UInt8) DEFAULT arrayMap(a -> greatest(COLUMNS('^a$'), 0), [1, 2])) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_capture (a UInt8, b Array(UInt8) DEFAULT arrayMap(p -> greatest(COLUMNS('^a$'), p), [1, 20])) ENGINE = Memory;
INSERT INTO t_capture (a) VALUES (10);
SELECT b FROM t_capture;
DROP TABLE t_capture;

SELECT '-- renaming a capturing lambda parameter keeps the column names of EXCEPT';
CREATE TABLE t_except (a UInt8, b Array(String) DEFAULT identity(COLUMNS('^a$') APPLY (x -> arrayMap(a -> toJSONString(tuple(COLUMNS('^a$') EXCEPT (a), x)), [1])))) ENGINE = Memory;
INSERT INTO t_except (a) VALUES (7);
SELECT b FROM t_except;
DROP TABLE t_except;

SELECT '-- a nested APPLY sees the parameter of the outer APPLY';
CREATE TABLE t_nested_apply (a UInt8, x UInt8, b String DEFAULT toJSONString(tuple(COLUMNS('^a$') APPLY (x -> tuple(COLUMNS('^a$') APPLY (y -> x + y)))))) ENGINE = Memory;
INSERT INTO t_nested_apply (a, x) VALUES (7, 100);
SELECT b FROM t_nested_apply;
DROP TABLE t_nested_apply;

SELECT '-- REPLACE substitutes the column, not a field access with the same name';
SET enable_named_columns_in_function_tuple = 0;
CREATE TABLE t_field (a UInt8, t Tuple(a UInt8), b String DEFAULT toJSONString(tuple(COLUMNS('^a$') REPLACE (a + 1 AS a) REPLACE (t.a AS a)))) ENGINE = Memory;
INSERT INTO t_field (a, t) VALUES (7, tuple(100));
SELECT b FROM t_field;
DROP TABLE t_field;

SELECT '-- a matcher is validated against the inferred and flattened Nested column';
SET flatten_nested = 1;
CREATE TABLE t_inferred (a UInt8, n DEFAULT CAST([], 'Nested(x UInt8)'), b UInt64 DEFAULT length(COLUMNS('^n$'))) ENGINE = Memory; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
