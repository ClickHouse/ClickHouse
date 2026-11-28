-- Trino `date_trunc('week', x)` starts weeks on Monday whatever `week_functions_starting_day` is, with the result type
-- of `dateTrunc`.

SET session_timezone = 'UTC';

SELECT 'native dateTrunc, auto';
SELECT dateTrunc('week', toDate('1970-01-01')), dateTrunc('week', toDate('1970-01-04')), dateTrunc('week', toDate('1970-01-05')),
    dateTrunc('week', toDate('2024-01-03')), dateTrunc('week', toDate('2024-01-07')), dateTrunc('week', toDate('2024-01-08'));

SELECT 'Trino date_trunc, weeks from Sunday';
SET week_functions_starting_day = 'sunday';
SET enable_trino_dialect = 1;
SET dialect = 'trino';
SELECT date_trunc('week', DATE '1970-01-01'), date_trunc('week', DATE '1970-01-04'), date_trunc('week', DATE '1970-01-05'),
    date_trunc('week', DATE '2024-01-03'), date_trunc('week', DATE '2024-01-07'), date_trunc('week', DATE '2024-01-08');
SELECT date_trunc('WEEK', DATE '2024-01-07') AS w, date_trunc('week', TIMESTAMP '2024-01-07 10:00:00');
SELECT toTypeName(date_trunc('week', DATE '2024-01-07')) = toTypeName(date_trunc('month', DATE '2024-01-07')),
    toTypeName(date_trunc('week', TIMESTAMP '2024-01-07 10:00:00')) = toTypeName(date_trunc('month', TIMESTAMP '2024-01-07 10:00:00'));
-- With the time zone argument of the native `dateTrunc`: 22:00 UTC on Sunday 2024-01-07 is 03:45 on Monday 2024-01-08 in
-- Kathmandu. `dateTrunc` in camel case is the native function, which is not rewritten and follows the setting.
SELECT date_trunc('week', TIMESTAMP '2024-01-07 22:00:00', 'Asia/Kathmandu'), dateTrunc('week', TIMESTAMP '2024-01-07 22:00:00', 'Asia/Kathmandu'),
    date_trunc('week', toDateTime64('2024-01-07 22:00:00', 3), 'Asia/Kathmandu');
SELECT toTypeName(date_trunc('week', TIMESTAMP '2024-01-07 22:00:00', 'Asia/Kathmandu')) = toTypeName(dateTrunc('week', TIMESTAMP '2024-01-07 22:00:00', 'Asia/Kathmandu')),
    toTypeName(date_trunc('week', toDateTime64('2024-01-07 22:00:00', 3), 'Asia/Kathmandu')) = toTypeName(dateTrunc('week', toDateTime64('2024-01-07 22:00:00', 3), 'Asia/Kathmandu'));
-- The other units are unchanged.
SELECT date_trunc('month', DATE '2024-01-17');
SET dialect = 'clickhouse';
