-- Rounding to the start of an hour must land on a wall clock hour, also on a day when the UTC offset
-- changes by a fraction of an hour. Australia/Lord_Howe shifts by 30 minutes twice a year.

SELECT 'clocks go back (2024-04-07, 02:00 -> 01:30)';
SELECT toStartOfHour(toDateTime('2024-04-07 01:10:00', 'Australia/Lord_Howe'));
SELECT toStartOfHour(toDateTime('2024-04-07 03:10:00', 'Australia/Lord_Howe'));
SELECT toStartOfHour(toDateTime('2024-04-07 12:58:30', 'Australia/Lord_Howe'));
SELECT toStartOfHour(toDateTime('2024-04-07 23:59:59', 'Australia/Lord_Howe'));
SELECT toStartOfHour(toDateTime('2024-04-08 05:20:00', 'Australia/Lord_Howe'));

SELECT 'clocks go forward (2024-10-06, 02:00 -> 02:30)';
SELECT toStartOfHour(toDateTime('2024-10-06 01:10:00', 'Australia/Lord_Howe'));
SELECT toStartOfHour(toDateTime('2024-10-06 06:00:00', 'Australia/Lord_Howe'));
SELECT toStartOfHour(toDateTime('2024-10-06 06:45:00', 'Australia/Lord_Howe'));
SELECT toStartOfHour(toDateTime('2024-10-06 23:59:59', 'Australia/Lord_Howe'));

SELECT 'the other ways to round to an hour agree';
SELECT dateTrunc('hour', toDateTime('2024-10-06 06:00:00', 'Australia/Lord_Howe'));
SELECT toStartOfInterval(toDateTime('2024-04-07 03:10:00', 'Australia/Lord_Howe'), INTERVAL 1 HOUR);
SELECT toStartOfInterval(toDateTime('2024-04-07 03:10:00', 'Australia/Lord_Howe'), INTERVAL 2 HOUR);
SELECT toStartOfHour(toDateTime64('2024-04-07 12:58:30.123', 3, 'Australia/Lord_Howe'));

SELECT 'every time after the shift keeps its hour and lands on :00';
SELECT countIf(toMinute(toStartOfHour(t)) != 0 OR toHour(toStartOfHour(t)) != toHour(t) OR toStartOfHour(t) > t OR t - toStartOfHour(t) >= 3600)
FROM (SELECT toDateTime('2024-04-07 03:00:00', 'Australia/Lord_Howe') + INTERVAL 7 * number MINUTE AS t FROM numbers(120));
SELECT countIf(toMinute(toStartOfHour(t)) != 0 OR toHour(toStartOfHour(t)) != toHour(t) OR toStartOfHour(t) > t OR t - toStartOfHour(t) >= 3600)
FROM (SELECT toDateTime('2024-10-06 03:00:00', 'Australia/Lord_Howe') + INTERVAL 7 * number MINUTE AS t FROM numbers(120));

SELECT 'the start never decreases and is at most an hour before the time, also in the repeated and skipped half hour';
SELECT countIf(s > u OR u - s >= 3600)
FROM (SELECT toUnixTimestamp(t) AS u, toUnixTimestamp(toStartOfHour(t)) AS s
      FROM (SELECT toTimeZone(toDateTime('2024-04-06 10:00:00', 'UTC') + number * 60, 'Australia/Lord_Howe') AS t FROM numbers(3 * 1440)));
SELECT countIf(s > u OR u - s >= 3600)
FROM (SELECT toUnixTimestamp(t) AS u, toUnixTimestamp(toStartOfHour(t)) AS s
      FROM (SELECT toTimeZone(toDateTime('2024-10-05 10:00:00', 'UTC') + number * 60, 'Australia/Lord_Howe') AS t FROM numbers(3 * 1440)));
SELECT countIf(s < p)
FROM (SELECT s, lagInFrame(s, 1, s) OVER (ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS p
      FROM (SELECT t, toUnixTimestamp(toStartOfHour(t)) AS s
            FROM (SELECT toTimeZone(toDateTime('2024-04-06 10:00:00', 'UTC') + number * 60, 'Australia/Lord_Howe') AS t FROM numbers(3 * 1440))));
SELECT countIf(s < p)
FROM (SELECT s, lagInFrame(s, 1, s) OVER (ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS p
      FROM (SELECT t, toUnixTimestamp(toStartOfHour(t)) AS s
            FROM (SELECT toTimeZone(toDateTime('2024-10-05 10:00:00', 'UTC') + number * 60, 'Australia/Lord_Howe') AS t FROM numbers(3 * 1440))));

SELECT 'a shift of more than an hour that is not a whole number of hours (Asia/Rangoon, 1945) is monotonic too';
SET enable_extended_results_for_datetime_functions = 1;
SELECT countIf(s > u OR u - s >= 3600)
FROM (SELECT toUnixTimestamp64Second(t) AS u, toUnixTimestamp64Second(toStartOfHour(t)) AS s
      FROM (SELECT toDateTime64(-778500000 + number * 60, 0, 'Asia/Rangoon') AS t FROM numbers(3000)));
SELECT countIf(s < p)
FROM (SELECT s, lagInFrame(s, 1, s) OVER (ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS p
      FROM (SELECT t, t, toUnixTimestamp64Second(toStartOfHour(t)) AS s
            FROM (SELECT toDateTime64(-778500000 + number * 60, 0, 'Asia/Rangoon') AS t FROM numbers(3000))));

SELECT 'dates outside the lookup table (after 2299) follow the offset changes too';
SELECT toStartOfHour(toDateTime64('2400-04-02 12:00:00', 0, 'Australia/Lord_Howe'));
SELECT toStartOfHour(toDateTime64('2400-10-01 12:00:00', 0, 'Australia/Lord_Howe'));
SELECT dateTrunc('hour', toDateTime64('2400-04-02 12:00:00', 0, 'Australia/Lord_Howe'));
SELECT toStartOfInterval(toDateTime64('2400-04-02 12:00:00', 0, 'Australia/Lord_Howe'), INTERVAL 1 HOUR);
SELECT countIf(s > u OR u - s >= 3600)
FROM (SELECT toUnixTimestamp64Second(t) AS u, toUnixTimestamp64Second(toStartOfHour(t)) AS s
      FROM (SELECT toTimeZone(toDateTime64('2400-04-01 10:00:00', 0, 'UTC') + toIntervalMinute(number), 'Australia/Lord_Howe') AS t FROM numbers(3 * 1440)));
SELECT countIf(s > u OR u - s >= 3600)
FROM (SELECT toUnixTimestamp64Second(t) AS u, toUnixTimestamp64Second(toStartOfHour(t)) AS s
      FROM (SELECT toTimeZone(toDateTime64('2400-09-30 10:00:00', 0, 'UTC') + toIntervalMinute(number), 'Australia/Lord_Howe') AS t FROM numbers(3 * 1440)));
SELECT countIf(s < p)
FROM (SELECT s, lagInFrame(s, 1, s) OVER (ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS p
      FROM (SELECT t, toUnixTimestamp64Second(toStartOfHour(t)) AS s
            FROM (SELECT toTimeZone(toDateTime64('2400-04-01 10:00:00', 0, 'UTC') + toIntervalMinute(number), 'Australia/Lord_Howe') AS t FROM numbers(3 * 1440))));

SELECT 'the hour starts at the offset change inside the repeated and skipped half hour after 2299, and rounding again keeps it';
WITH toDateTime64('2400-10-01 02:40:00', 0, 'Australia/Lord_Howe') AS t
SELECT toStartOfHour(t), toStartOfHour(toStartOfHour(t)), dateTrunc('hour', t), toStartOfInterval(t, INTERVAL 1 HOUR);
WITH toTimeZone(toDateTime64('2400-04-01 15:10:00', 0, 'UTC'), 'Australia/Lord_Howe') AS t
SELECT toTimeZone(toStartOfHour(t), 'UTC'), toTimeZone(toStartOfHour(toStartOfHour(t)), 'UTC');
WITH toTimeZone(toDateTime64('2400-04-01 14:50:00', 0, 'UTC'), 'Australia/Lord_Howe') AS t
SELECT toTimeZone(toStartOfHour(t), 'UTC');

SELECT 'the start of the hour is a whole hour on the clock or the offset change itself, and rounding it again keeps it';
SELECT countIf(toStartOfHour(s) != s OR NOT (toMinute(s) = 0 AND toSecond(s) = 0 OR timeZoneOffset(s) != timeZoneOffset(s - 1)))
FROM (SELECT toStartOfHour(t) AS s
      FROM (SELECT toTimeZone(toDateTime64(start, 0, 'UTC') + toIntervalMinute(number), 'Australia/Lord_Howe') AS t
            FROM numbers(3 * 1440)
            ARRAY JOIN ['2024-04-06 10:00:00', '2024-10-05 10:00:00', '2400-04-01 10:00:00', '2400-09-30 10:00:00'] AS start));

SELECT 'an offset change by a fraction of an hour that skips the local midnight before 1900 (Australia/Adelaide, 1899-05-01)';
WITH toDateTime64('1899-05-01 12:00:00', 0, 'Australia/Adelaide') AS t
SELECT toStartOfHour(t), dateTrunc('hour', t), toStartOfInterval(t, INTERVAL 1 HOUR);
WITH toDateTime64('1899-05-01 00:40:00', 0, 'Australia/Adelaide') AS t
SELECT toStartOfHour(t), toStartOfHour(toStartOfHour(t));
SELECT toStartOfHour(toDateTime64('1899-04-30 23:40:00', 0, 'Australia/Adelaide'));
SELECT countIf(s > u OR u - s >= 3600)
FROM (SELECT toUnixTimestamp64Second(t) AS u, toUnixTimestamp64Second(toStartOfHour(t)) AS s
      FROM (SELECT toTimeZone(toDateTime64('1899-04-29 10:00:00', 0, 'UTC') + toIntervalMinute(number), 'Australia/Adelaide') AS t FROM numbers(3 * 1440)));
SELECT countIf(s < p)
FROM (SELECT s, lagInFrame(s, 1, s) OVER (ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS p
      FROM (SELECT t, toUnixTimestamp64Second(toStartOfHour(t)) AS s
            FROM (SELECT toTimeZone(toDateTime64('1899-04-29 10:00:00', 0, 'UTC') + toIntervalMinute(number), 'Australia/Adelaide') AS t FROM numbers(3 * 1440))));
SELECT countIf(toStartOfHour(s) != s OR NOT (toMinute(s) = 0 AND toSecond(s) = 0 OR timeZoneOffset(s) != timeZoneOffset(s - 1)))
FROM (SELECT toStartOfHour(t) AS s
      FROM (SELECT toTimeZone(toDateTime64('1899-04-29 10:00:00', 0, 'UTC') + toIntervalMinute(number), 'Australia/Adelaide') AS t FROM numbers(3 * 1440)));
SET enable_extended_results_for_datetime_functions = 0;

SELECT 'time windows are a fixed hour long, so each one contains its time';
SELECT countIf(NOT (tumbleStart(t, INTERVAL 1 HOUR) <= t AND t < tumbleEnd(t, INTERVAL 1 HOUR)))
FROM (SELECT toTimeZone(toDateTime('2024-04-06 10:00:00', 'UTC') + number * 60, 'Australia/Lord_Howe') AS t FROM numbers(3 * 1440));
SELECT countIf(NOT (tumbleStart(t, INTERVAL 1 HOUR) <= t AND t < tumbleEnd(t, INTERVAL 1 HOUR)))
FROM (SELECT toTimeZone(toDateTime('2024-10-05 10:00:00', 'UTC') + number * 60, 'Australia/Lord_Howe') AS t FROM numbers(3 * 1440));
SELECT countIf(NOT (hopStart(t, INTERVAL 1 HOUR, INTERVAL 2 HOUR) <= t AND t < hopEnd(t, INTERVAL 1 HOUR, INTERVAL 2 HOUR)))
FROM (SELECT toTimeZone(toDateTime('2024-04-06 10:00:00', 'UTC') + number * 60, 'Australia/Lord_Howe') AS t FROM numbers(3 * 1440));
SELECT countIf(tumbleEnd(t, INTERVAL 1 HOUR) - tumbleStart(t, INTERVAL 1 HOUR) != 3600)
FROM (SELECT toTimeZone(toDateTime('2024-10-05 10:00:00', 'UTC') + number * 60, 'Australia/Lord_Howe') AS t FROM numbers(3 * 1440));

SELECT 'zones whose shift is a whole number of hours are unchanged';
SELECT toStartOfHour(toDateTime('2024-03-31 03:40:00', 'Europe/Berlin'));
SELECT toStartOfHour(toDateTime('2024-10-27 03:40:00', 'Europe/Berlin'));
SELECT toStartOfHour(toDateTime('2024-04-07 12:58:30', 'Australia/Adelaide'));
SELECT toStartOfHour(toDateTime('2024-04-07 12:58:30', 'Asia/Kolkata'));
