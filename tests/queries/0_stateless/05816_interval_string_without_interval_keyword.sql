-- An interval can be written as a string without the `INTERVAL` keyword, as in PostgreSQL: `date_time + '30 days'`.
-- The rewrite is done by the analyzer.
SET enable_analyzer = 1;

SELECT 'same as with INTERVAL';
WITH toDateTime('2026-01-01 00:00:00', 'UTC') AS dt
SELECT
    dt + '30 days' = dt + INTERVAL '30 days',
    dt + '30 day' = dt + INTERVAL '30 day',
    dt + '1 hour' = dt + INTERVAL '1 hour',
    dt + '1 day 2 hours' = dt + INTERVAL '1 day 2 hours',
    dt + '-3 days' = dt + INTERVAL '-3 days',
    dt + '1.5 day' = dt + INTERVAL '1.5 day',
    dt + '2 MONTHS' = dt + INTERVAL '2 MONTHS',
    dt - '30 days' = dt - INTERVAL '30 days',
    '30 days' + dt = INTERVAL '30 days' + dt;

SELECT 'values';
WITH toDateTime('2026-01-01 00:00:00', 'UTC') AS dt
SELECT dt + '30 days', dt - '30 days', '30 days' + dt, dt + '1 day 2 hours';

SELECT 'types';
SELECT toDate('2026-01-01') + '30 days', toTypeName(toDate('2026-01-01') + '30 days');
SELECT toDate32('2026-01-01') + '30 days', toTypeName(toDate32('2026-01-01') + '30 days');
SELECT toDateTime64('2026-01-01 00:00:00', 3, 'UTC') + '1 second', toTypeName(toDateTime64('2026-01-01 00:00:00', 3, 'UTC') + '1 second');
SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + '5 milliseconds', toTypeName(toDateTime('2026-01-01 00:00:00', 'UTC') + '5 milliseconds');
SELECT toNullable(toDateTime('2026-01-01 00:00:00', 'UTC')) + '1 day', toTypeName(toNullable(toDateTime('2026-01-01 00:00:00', 'UTC')) + '1 day');

SELECT 'column name';
SELECT toDate('2026-01-01') + '30 days' FORMAT TSVWithNames;

SELECT 'table';
DROP TABLE IF EXISTS t_interval_string;
CREATE TABLE t_interval_string (d DateTime('UTC'), n Nullable(Date)) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_interval_string VALUES ('2026-01-01 00:00:00', '2026-01-01'), ('2026-02-01 00:00:00', NULL);
SELECT d + '1 week', n - '1 day' FROM t_interval_string ORDER BY d;
SELECT count() FROM t_interval_string WHERE d > toDateTime('2026-02-01 00:00:00', 'UTC') - '1 day';

SELECT 'distributed';
SELECT d + '1 day' FROM remote('127.0.0.2', currentDatabase(), t_interval_string) ORDER BY d;
DROP TABLE t_interval_string;

SELECT 'errors';
-- Not an interval: the string is left as is and the query fails as before.
SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + 'abc'; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + '30'; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + '30 days ago'; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + ''; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- An interval cannot be the first argument of `minus`.
SELECT '30 days' - toDateTime('2026-01-01 00:00:00', 'UTC'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- Only a string literal is converted, as with `INTERVAL`.
SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + materialize('30 days'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + concat('30 ', 'days'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- The other argument must be a date or time.
SELECT 1 + '30 days'; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT '2026-01-01' + '30 days'; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
