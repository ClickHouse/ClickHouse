-- Bracketed expressions and query parameters in `VALUES` are evaluated for a `Dynamic` column, as for other types.
SET enable_dynamic_type = 1;
SET input_format_values_interpret_expressions = 1;
SET param_x = 5;

DROP TABLE IF EXISTS t_dynamic_values_bracketed_expr;
CREATE TABLE t_dynamic_values_bracketed_expr (id UInt8, d Dynamic) ENGINE = Memory;
INSERT INTO t_dynamic_values_bracketed_expr VALUES (1, (1 + 1)), (2, [1 + 1]), (3, {x:UInt8}), (4, 1 + 1), (5, [1, 2]), (6, (1, 'a'));
SELECT id, toString(d), dynamicType(d) = 'String' FROM t_dynamic_values_bracketed_expr ORDER BY id;
DROP TABLE t_dynamic_values_bracketed_expr;

-- Bracketed literals without an inferable type and expressions that cannot be evaluated are still stored as `String`.
CREATE TABLE t_dynamic_values_bracketed_expr (id UInt8, d Dynamic) ENGINE = Memory;
INSERT INTO t_dynamic_values_bracketed_expr VALUES (1, {}), (2, []), (3, ()), (4, [1 + 'a']), (5, (1 + 1));
SELECT id, toString(d), dynamicType(d) = 'String' FROM t_dynamic_values_bracketed_expr ORDER BY id;
DROP TABLE t_dynamic_values_bracketed_expr;
