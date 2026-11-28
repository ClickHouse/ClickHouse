-- An integer constant that the `values` table function or the `INSERT ... VALUES` expression fallback stores into
-- a `Date`, `Date32`, `DateTime` or `Time` column follows `date_time_overflow_behavior` in the `throw` and `saturate`
-- modes, like `CAST` of the same constant. The default `ignore` mode keeps rejecting it.

SET session_timezone = 'UTC';

SELECT '-- ignore: unchanged';
SET date_time_overflow_behavior = 'ignore';
SELECT c FROM values('c Date', 65536); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT c FROM values('c Date32', 999999999999); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT c FROM values('c DateTime', 4294967296); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('d Date, d32 Date32, dt DateTime', (65535, -719528, 4294967295));

SELECT '-- saturate: the same as CAST';
SET date_time_overflow_behavior = 'saturate';
SELECT c, c = CAST(65535 AS Date) FROM values('c Date', 65535);
SELECT c, c = CAST(65536 AS Date) FROM values('c Date', 65536);
SELECT c, c = CAST(99999999999 AS Date) FROM values('c Date', 99999999999);
SELECT c, c = CAST(-1 AS Date) FROM values('c Date', -1);
SELECT c, c = CAST(2932896 AS Date32) FROM values('c Date32', 2932896);
SELECT c, c = CAST(2932897 AS Date32) FROM values('c Date32', 2932897);
SELECT c, c = CAST(999999999999 AS Date32) FROM values('c Date32', 999999999999);
SELECT c, c = CAST(18446744073709551615 AS Date32) FROM values('c Date32', 18446744073709551615);
SELECT c, c = CAST(-719528 AS Date32) FROM values('c Date32', -719528);
SELECT c, c = CAST(-719529 AS Date32) FROM values('c Date32', -719529);
SELECT c, c = CAST(4294967295 AS DateTime) FROM values('c DateTime', 4294967295);
SELECT c, c = CAST(4294967296 AS DateTime) FROM values('c DateTime', 4294967296);
SELECT c, c = CAST(18446744073709551615 AS DateTime) FROM values('c DateTime', 18446744073709551615);
SELECT c, c = CAST(-1 AS DateTime) FROM values('c DateTime', -1);
SELECT toInt32(c), c = CAST(3599999 AS Time) FROM values('c Time', 3599999);
SELECT toInt32(c), c = CAST(3600000 AS Time) FROM values('c Time', 3600000);
SELECT toInt32(c), c = CAST(-3600000 AS Time) FROM values('c Time', -3600000);
SELECT c FROM values('c Nullable(Date)', 99999999999, NULL);
SELECT c FROM values('c Array(DateTime)', [1, 4294967296]);

SELECT '-- throw';
SET date_time_overflow_behavior = 'throw';
SELECT * FROM values('d Date, d32 Date32, dt DateTime', (65535, -719528, 4294967295));
SELECT toInt32(c) FROM values('c Time', -3599999, 3599999);
-- Not an overflow: a `Date` constant above the greatest day number is a unix timestamp, as in `CAST`.
SELECT c, c = CAST(65536 AS Date) FROM values('c Date', 65536);
SELECT c FROM values('c Date', 99999999999); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT c FROM values('c Date', -1); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT c FROM values('c Date32', 999999999999); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT c FROM values('c Date32', -719529); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT c FROM values('c DateTime', 4294967296); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT c FROM values('c DateTime', -1); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT c FROM values('c Time', 3600000); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT c FROM values('c Time', -3600000); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '-- INSERT ... VALUES with an expression';
-- Without the templates the expression is evaluated to a constant and converted as a `Field`.
SET input_format_values_deduce_templates_of_expressions = 0;
CREATE TABLE t_overflow_values (d Date, d32 Date32, dt DateTime, t Time) ENGINE = Memory;
INSERT INTO t_overflow_values VALUES (99999999999 + 0, 0, 0, 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_overflow_values VALUES (0, 999999999999 + 0, 0, 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_overflow_values VALUES (0, 0, 4294967296 + 0, 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_overflow_values VALUES (0, 0, 0, 3600000 + 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_overflow_values VALUES (1 + 0, 1 + 0, 1 + 0, 1 + 0);
SET date_time_overflow_behavior = 'saturate';
INSERT INTO t_overflow_values VALUES (99999999999 + 0, 999999999999 + 0, 4294967296 + 0, 3600000 + 0);
INSERT INTO t_overflow_values VALUES (-1 + 0, -999999999999 + 0, -1 + 0, -3600000 + 0);
SELECT d, d32, dt, toInt32(t) FROM t_overflow_values ORDER BY ALL;
DROP TABLE t_overflow_values;

SELECT '-- an exact constant is never clamped';
SELECT toDateTime(4294967295) IN (4294967296), toDate(65535) IN (99999999999), toDate32('9999-12-31') IN (999999999999);
