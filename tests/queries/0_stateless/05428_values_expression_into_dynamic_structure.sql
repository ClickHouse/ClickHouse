-- The expression fallback of `INSERT ... VALUES` into a destination with dynamic structure renders a
-- `JSON` / `Dynamic` member converted to `String` as `CAST` does, with and without template deduction,
-- and with `NULL` members replaced with defaults.
DROP TABLE IF EXISTS t_05428;
CREATE TABLE t_05428 (n UInt8, v Tuple(String, Dynamic), a Array(Tuple(String, Dynamic))) ENGINE = Memory;
SET input_format_null_as_default = 1;
SET input_format_values_deduce_templates_of_expressions = 0;
INSERT INTO t_05428 VALUES (1, (CAST('{"d":"2020-01-01"}', 'JSON'), 1), []);
INSERT INTO t_05428 VALUES (2, (CAST([toDate('2020-01-01')], 'Array(Dynamic)'), toDate('2020-01-02')), []);
INSERT INTO t_05428 VALUES (3, ('x', 1), [(CAST('{"d":"2020-01-01"}', 'JSON'), toDate('2020-01-02')), (NULL, NULL)]);
INSERT INTO t_05428 VALUES (4, toNullable(tuple(CAST('{"d":"2020-01-01"}', 'JSON'), 1)), []);
SET input_format_values_deduce_templates_of_expressions = 1;
INSERT INTO t_05428 VALUES (5, (CAST('{"d":"2020-01-01"}', 'JSON'), 1), []);
SELECT n, v, dynamicType(v.2), a, arrayMap(x -> dynamicType(x.2), a) FROM t_05428 ORDER BY n;
DROP TABLE t_05428;
