-- `parseDateTime` converts a local time as `toDateTime` does, also next to a daylight saving time change.
-- In `Europe/Berlin` 02:00-02:59 happens twice on 2026-10-25 and does not exist on 2026-03-29.
-- https://github.com/ClickHouse/ClickHouse/issues/126667

SELECT parseDateTime('2026-10-25 01:00:00', '%Y-%m-%d %H:%i:%s', 'Europe/Berlin');

SELECT 'Europe/Berlin, clocks go back';
SELECT s, toUnixTimestamp(parseDateTime(s, '%Y-%m-%d %H:%i:%s', 'Europe/Berlin')) AS t, t = toUnixTimestamp(toDateTime(s, 'Europe/Berlin'))
FROM (SELECT formatDateTime(toDateTime('2026-10-25 00:00:00', 'UTC') + number * 1800, '%Y-%m-%d %H:%i:%s', 'UTC') AS s FROM numbers(10));

SELECT 'Europe/Berlin, clocks go forward';
SELECT s, toUnixTimestamp(parseDateTime(s, '%Y-%m-%d %H:%i:%s', 'Europe/Berlin')) AS t, t = toUnixTimestamp(toDateTime(s, 'Europe/Berlin'))
FROM (SELECT formatDateTime(toDateTime('2026-03-29 00:00:00', 'UTC') + number * 1800, '%Y-%m-%d %H:%i:%s', 'UTC') AS s FROM numbers(10));

-- West of UTC, the hours after the change were wrong.
SELECT 'America/New_York, clocks go back';
SELECT s, toUnixTimestamp(parseDateTime(s, '%Y-%m-%d %H:%i:%s', 'America/New_York')) AS t, t = toUnixTimestamp(toDateTime(s, 'America/New_York'))
FROM (SELECT formatDateTime(toDateTime('2026-11-01 00:00:00', 'UTC') + number * 1800, '%Y-%m-%d %H:%i:%s', 'UTC') AS s FROM numbers(10));

SELECT 'other functions and formats';
SELECT parseDateTime64('2026-10-25 01:00:00.5', '%Y-%m-%d %H:%i:%s.%f', 'Europe/Berlin');
SELECT parseDateTimeInJodaSyntax('2026-10-25 01:00:00', 'yyyy-MM-dd HH:mm:ss', 'Europe/Berlin');
SELECT parseDateTimeOrNull('2026-10-25 01:00:00', '%Y-%m-%d %H:%i:%s', 'Europe/Berlin');
SELECT parseDateTime('2026-10-25 01:00:00 AM', '%Y-%m-%d %h:%i:%s %p', 'Europe/Berlin');
SELECT parseDateTime('2026-298 01:00:00', '%Y-%j %H:%i:%s', 'Europe/Berlin');
SELECT parseDateTimeInJodaSyntax('2026-43-7 01:00:00', 'xxxx-ww-e HH:mm:ss', 'Europe/Berlin');

-- An offset in the string is used as it is.
SELECT parseDateTime('2026-10-25 01:00:00 +0100', '%Y-%m-%d %H:%i:%s %z', 'Europe/Berlin');

-- A time zone name in the string gives the same results, also for a local time that does not exist.
SELECT 'America/New_York, clocks go forward, time zone name in the string';
SELECT s, toUnixTimestamp(parseDateTimeInJodaSyntax(concat(s, ' America/New_York'), 'yyyy-MM-dd HH:mm:ss z', 'UTC')) AS t,
    t = toUnixTimestamp(toDateTime(s, 'America/New_York'))
FROM (SELECT formatDateTime(toDateTime('2026-03-08 00:00:00', 'UTC') + number * 1800, '%Y-%m-%d %H:%i:%s', 'UTC') AS s FROM numbers(10));
SELECT parseDateTime64InJodaSyntaxOrNull('2026-03-08 02:30:00 America/New_York', 'yyyy-MM-dd HH:mm:ss z', 'UTC');
SELECT parseDateTimeInJodaSyntax('2026-10-25 02:30:00 Europe/Berlin', 'yyyy-MM-dd HH:mm:ss z', 'UTC');

-- An offset before the time zone name selects one of the two 02:30.
SELECT parseDateTimeInJodaSyntax('2026-10-25 02:30:00 +0200 Europe/Berlin', 'yyyy-MM-dd HH:mm:ss Z z', 'UTC');
SELECT parseDateTimeInJodaSyntax('2026-10-25 02:30:00 +0100 Europe/Berlin', 'yyyy-MM-dd HH:mm:ss Z z', 'UTC');
SELECT parseDateTime64InJodaSyntax('2026-10-25 02:30:00.5 +0100 Europe/Berlin', 'yyyy-MM-dd HH:mm:ss.S Z z', 'UTC');

-- A time zone name in the string builds the time once more, which added the 12 hours of PM twice.
SELECT 'PM with a time zone name in the string';
SELECT parseDateTimeInJodaSyntax('2026-10-25 01:30:00 PM Europe/Berlin', 'yyyy-MM-dd hh:mm:ss a z', 'UTC');
SELECT parseDateTimeInJodaSyntax('2026-10-25 12:30:00 PM Europe/Berlin', 'yyyy-MM-dd hh:mm:ss a z', 'UTC');
SELECT parseDateTimeInJodaSyntax('2026-10-25 12:30:00 AM Europe/Berlin', 'yyyy-MM-dd hh:mm:ss a z', 'UTC');
SELECT parseDateTime64InJodaSyntax('2026-10-25 01:30:00.5 PM Europe/Berlin', 'yyyy-MM-dd hh:mm:ss.S a z', 'UTC');
SELECT parseDateTimeInJodaSyntax('2026-03-10 01:30:00 PM UTC', 'yyyy-MM-dd hh:mm:ss a z', 'UTC');
SELECT parseDateTimeInJodaSyntax('2026-10-25 01:30:00 PM +0100 Europe/Berlin', 'yyyy-MM-dd hh:mm:ss a Z z', 'UTC');
