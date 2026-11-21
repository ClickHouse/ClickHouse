-- A `DateTime` / `DateTime64` member of a composite constant on the right-hand side of `IN` is not
-- representable as a `Date` / `Date32` when it has a time of day: such a set member must not be
-- truncated to its day, so `IN` agrees with `=`. Midnight is representable.

SELECT [toDate('2020-01-01')] IN ([toDateTime('2020-01-01 12:00:00', 'UTC')]), [toDate('2020-01-01')] = [toDateTime('2020-01-01 12:00:00', 'UTC')];
SELECT [toDate('2020-01-01')] IN ([toDateTime('2020-01-01 00:00:00', 'UTC')]), [toDate('2020-01-01')] = [toDateTime('2020-01-01 00:00:00', 'UTC')];
SELECT [toDate32('2020-01-01')] IN ([toDateTime('2020-01-01 12:00:00', 'UTC')]), [toDate32('2020-01-01')] IN ([toDateTime('2020-01-01 00:00:00', 'UTC')]);
SELECT tuple(toDate('2020-01-01')) IN (tuple(toDateTime('2020-01-01 12:00:00', 'UTC'))), tuple(toDate('2020-01-01')) IN (tuple(toDateTime('2020-01-01 00:00:00', 'UTC')));
SELECT map(1, toDate('2020-01-01')) IN (map(1, toDateTime('2020-01-01 12:00:00', 'UTC'))), map(1, toDate('2020-01-01')) IN (map(1, toDateTime('2020-01-01 00:00:00', 'UTC')));
SELECT [toDate('2020-01-01')] IN ([toDateTime('2020-01-01 00:00:00', 'Asia/Tokyo')]), [toDate('2020-01-01')] IN ([toDateTime('2020-01-01 09:00:00', 'Asia/Tokyo')]);
SELECT [toDate('2020-01-01')] IN ([toDateTime64('2020-01-01 12:00:00', 3, 'UTC')]), [toDate('2020-01-01')] IN ([toDateTime64('2020-01-01 00:00:00.5', 3, 'UTC')]), [toDate('2020-01-01')] IN ([toDateTime64('2020-01-01 00:00:00', 3, 'UTC')]);
SELECT [toDate32('2020-01-01')] IN ([toDateTime64('2020-01-01 12:00:00', 3, 'UTC')]), [toDate32('2020-01-01')] IN ([toDateTime64('2020-01-01 00:00:00', 3, 'UTC')]);

DROP TABLE IF EXISTS t_nested_date;
CREATE TABLE t_nested_date (a Array(Date)) ENGINE = Memory;
INSERT INTO t_nested_date VALUES (['2020-01-01']);
SELECT count() FROM t_nested_date WHERE a IN ([toDateTime('2020-01-01 12:00:00', 'UTC')]);
SELECT count() FROM t_nested_date WHERE a NOT IN ([toDateTime('2020-01-01 12:00:00', 'UTC')]);
SELECT count() FROM t_nested_date WHERE a IN ([toDateTime('2020-01-01 00:00:00', 'UTC')]);
DROP TABLE t_nested_date;

-- Materializing a value still truncates it to its day, as `CAST` does.
SELECT CAST([toDateTime('2020-01-01 12:00:00', 'UTC')], 'Array(Date)');
SELECT * FROM values('a Array(Date)', ([toDateTime('2020-01-01 12:00:00', 'UTC')]));

-- `NULL` members replaced with the default of the destination type by `input_format_null_as_default`
-- are not reinterpreted as values of the source type.
SET input_format_null_as_default = 1;
SET input_format_values_deduce_templates_of_expressions = 0;
DROP TABLE IF EXISTS t_values_temporal_default;
CREATE TABLE t_values_temporal_default (v Tuple(String, DateTime64(3, 'UTC')), w Tuple(String, DateTime('America/New_York')), d Tuple(String, Date)) ENGINE = Memory;
INSERT INTO t_values_temporal_default VALUES (('keep', CAST(NULL, 'Nullable(Date)')), ('keep', CAST(NULL, 'Nullable(Date)')), ('keep', CAST(NULL, 'Nullable(DateTime64(3))')));
INSERT INTO t_values_temporal_default VALUES (('keep', CAST('2020-01-01', 'Nullable(Date)')), ('keep', CAST('2020-01-01', 'Nullable(Date)')), ('keep', CAST('2020-01-01 12:00:00', 'Nullable(DateTime64(3, \'UTC\'))')));
SELECT v, toUInt32(w.2), d FROM t_values_temporal_default ORDER BY v;
DROP TABLE t_values_temporal_default;

DROP TABLE IF EXISTS t_values_temporal_default_dynamic;
CREATE TABLE t_values_temporal_default_dynamic (v Tuple(String, DateTime64(3, 'UTC'), Dynamic)) ENGINE = Memory;
INSERT INTO t_values_temporal_default_dynamic VALUES (('keep', CAST(NULL, 'Nullable(Date)'), 1));
SELECT v FROM t_values_temporal_default_dynamic;
DROP TABLE t_values_temporal_default_dynamic;
