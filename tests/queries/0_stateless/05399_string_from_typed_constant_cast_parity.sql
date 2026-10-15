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
