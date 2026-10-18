-- A `DateTime64` / `Time64` constant converted to `String` for `IN` or `values` follows
-- `date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands` as `CAST` does.
SET enable_time_time64_type = 1;
SET date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 1;

SELECT CAST(toDateTime64('2020-01-01 00:00:00', 6, 'UTC'), 'String');
SELECT '2020-01-01 00:00:00' IN (toDateTime64('2020-01-01 00:00:00', 6, 'UTC')), '2020-01-01 00:00:00.000000' IN (toDateTime64('2020-01-01 00:00:00', 6, 'UTC'));
SELECT x FROM values('x String', toDateTime64('2020-01-01 00:00:00', 6, 'UTC'));

SELECT CAST(toTime64('12:00:00.120000', 6), 'String');
SELECT '12:00:00.120' IN (toTime64('12:00:00.120000', 6)), '12:00:00.120000' IN (toTime64('12:00:00.120000', 6));
SELECT x FROM values('x String', toTime64('12:00:00.120000', 6));

SET date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 0;
SELECT '2020-01-01 00:00:00.000000' IN (toDateTime64('2020-01-01 00:00:00', 6, 'UTC'));

-- The expression fallback of `INSERT ... VALUES` renders a `JSON` / `Dynamic` value as `CAST` does,
-- also when the template deduction is disabled.
DROP TABLE IF EXISTS t_05399;
CREATE TABLE t_05399 (n UInt8, s String) ENGINE = Memory;
SET input_format_values_deduce_templates_of_expressions = 0;
INSERT INTO t_05399 VALUES (1, CAST('{"d":"2020-01-01"}', 'JSON'));
INSERT INTO t_05399 VALUES (2, [CAST(true, 'Dynamic')]::Array(Dynamic));
SET input_format_values_deduce_templates_of_expressions = 1;
INSERT INTO t_05399 VALUES (3, CAST('{"d":"2020-01-01"}', 'JSON'));
SELECT n, s, s = CAST(CAST('{"d":"2020-01-01"}', 'JSON'), 'String') FROM t_05399 WHERE n != 2 ORDER BY n;
SELECT n, s FROM t_05399 WHERE n = 2;
DROP TABLE t_05399;

-- With `input_format_null_as_default`, a `NULL` member replaced with the default does not make the
-- other members lose their source types.
DROP TABLE IF EXISTS t_05399_defaults;
CREATE TABLE t_05399_defaults (n UInt8, v Tuple(String, UInt8), a Array(String), m Map(String, UInt8)) ENGINE = Memory;
SET input_format_null_as_default = 1;
SET input_format_values_deduce_templates_of_expressions = 0;
INSERT INTO t_05399_defaults VALUES (1, (CAST('{"d":"2020-01-01"}', 'JSON'), NULL), [], {});
INSERT INTO t_05399_defaults VALUES (2, ('x', 1), CAST([toDate('2020-01-01'), NULL], 'Array(Dynamic)'), {});
INSERT INTO t_05399_defaults VALUES (3, ('x', 1), [], map(CAST(toDate('2020-01-01'), 'Dynamic'), NULL));
SET input_format_values_deduce_templates_of_expressions = 1;
INSERT INTO t_05399_defaults VALUES (4, (CAST('{"d":"2020-01-01"}', 'JSON'), NULL), [], {});
-- The same with the composite held in a carrier: `Nullable`, `Dynamic`, `Variant`.
SET input_format_values_deduce_templates_of_expressions = 0;
INSERT INTO t_05399_defaults VALUES (5, toNullable(tuple(CAST('{"d":"2020-01-01"}', 'JSON'), NULL)), [], {});
INSERT INTO t_05399_defaults VALUES (6, ('x', 1), CAST(CAST([toDate('2020-01-01'), NULL], 'Array(Dynamic)'), 'Dynamic'), {});
INSERT INTO t_05399_defaults VALUES (7, ('x', 1), CAST(CAST([toDate('2020-01-01'), NULL], 'Array(Dynamic)'), 'Variant(Array(Dynamic), UInt8)'), {});
SET input_format_values_deduce_templates_of_expressions = 1;
INSERT INTO t_05399_defaults VALUES (8, toNullable(tuple(CAST('{"d":"2020-01-01"}', 'JSON'), NULL)), [], {});
SELECT * FROM t_05399_defaults ORDER BY n;
DROP TABLE t_05399_defaults;
