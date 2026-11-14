-- A `DateTime` / `DateTime64` member of a composite constant on the right-hand side of `IN` that is carried
-- inside `Dynamic` / `Variant` is not representable as a `Date` when it has a time of day, as for a plain
-- member: `IN` must agree with `=`. Midnight is representable.

SET allow_experimental_dynamic_type = 1, allow_experimental_variant_type = 1;

SELECT [toDate('2020-01-01')] IN (CAST([toDateTime('2020-01-01 12:00:00', 'UTC')], 'Array(Dynamic)'));
SELECT [toDate('2020-01-01')] IN (CAST([toDateTime('2020-01-01 00:00:00', 'UTC')], 'Array(Dynamic)'));
SELECT [toDate('2020-01-01')] IN (CAST([toDateTime('2020-01-01 12:00:00', 'UTC')], 'Array(Variant(DateTime(\'UTC\'), String))'));
SELECT [toDate('2020-01-01')] IN (CAST([toDateTime('2020-01-01 00:00:00', 'UTC')], 'Array(Variant(DateTime(\'UTC\'), String))'));
SELECT [toDate('2020-01-01')] IN (CAST([toDateTime64('2020-01-01 12:00:00', 3, 'UTC')], 'Array(Dynamic)'));
SELECT [toDate('2020-01-01')] IN (CAST([toDateTime64('2020-01-01 00:00:00', 3, 'UTC')], 'Array(Dynamic)'));
SELECT map(1, toDate('2020-01-01')) IN (CAST(map(1, toDateTime('2020-01-01 12:00:00', 'UTC')), 'Map(UInt8, Dynamic)'));
SELECT map(1, toDate('2020-01-01')) IN (CAST(map(1, toDateTime('2020-01-01 00:00:00', 'UTC')), 'Map(UInt8, Dynamic)'));

DROP TABLE IF EXISTS t_nested_date_dynamic;
CREATE TABLE t_nested_date_dynamic (a Array(Date)) ENGINE = Memory;
INSERT INTO t_nested_date_dynamic VALUES (['2020-01-01']);
SELECT count() FROM t_nested_date_dynamic WHERE a IN (CAST([toDateTime('2020-01-01 12:00:00', 'UTC')], 'Array(Dynamic)'));
SELECT count() FROM t_nested_date_dynamic WHERE a NOT IN (CAST([toDateTime('2020-01-01 12:00:00', 'UTC')], 'Array(Dynamic)'));
SELECT count() FROM t_nested_date_dynamic WHERE a IN (CAST([toDateTime('2020-01-01 00:00:00', 'UTC')], 'Array(Dynamic)'));
DROP TABLE t_nested_date_dynamic;
