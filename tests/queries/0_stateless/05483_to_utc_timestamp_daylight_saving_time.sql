-- `toUTCTimestamp` converts a local time to UTC as `toDateTime(s, time_zone)` does, also next to a daylight saving time change.
-- In `Europe/Berlin` 02:00-02:59 happens twice on 2026-10-25 and does not exist on 2026-03-29.
-- https://github.com/ClickHouse/ClickHouse/issues/128266

SET session_timezone = 'UTC';

SELECT toUTCTimestamp(toDateTime('2026-10-25 01:00:00'), 'Europe/Berlin');
SELECT toUTCTimestamp(toDateTime64('2026-10-25 01:00:00.5', 1), 'Europe/Berlin');
SELECT toUTCTimestamp(toDateTime('2026-11-01 02:30:00'), 'America/New_York');
SELECT toUTCTimestamp(toDateTime('2026-03-08 04:00:00'), 'America/New_York');
SELECT fromUTCTimestamp(toUTCTimestamp(toDateTime('2026-03-08 04:00:00'), 'America/New_York'), 'America/New_York');
-- The value is read as a UTC wall clock in any session time zone.
SELECT to_utc_timestamp(toDateTime('2026-10-25 04:00:00'), 'Europe/Berlin') SETTINGS session_timezone = 'Europe/Moscow';

SELECT 'Europe/Berlin';
SELECT s, toUTCTimestamp(toDateTime(s), 'Europe/Berlin') AS t, fromUTCTimestamp(t, 'Europe/Berlin'),
    toUnixTimestamp(t) = toUnixTimestamp(toDateTime(s, 'Europe/Berlin'))
FROM (SELECT formatDateTime(toDateTime(d) + n * 1800, '%Y-%m-%d %H:%i:%s') AS s
      FROM (SELECT arrayJoin(['2026-03-29', '2026-10-25']) AS d, arrayJoin(range(10)) AS n))
ORDER BY s;

-- West of UTC the hours after the change were wrong.
SELECT 'America/New_York';
SELECT s, toUTCTimestamp(toDateTime(s), 'America/New_York') AS t, fromUTCTimestamp(t, 'America/New_York'),
    toUnixTimestamp(t) = toUnixTimestamp(toDateTime(s, 'America/New_York'))
FROM (SELECT formatDateTime(toDateTime(d) + n * 1800, '%Y-%m-%d %H:%i:%s') AS s
      FROM (SELECT arrayJoin(['2026-03-08', '2026-11-01']) AS d, arrayJoin(range(16)) AS n))
ORDER BY s;

-- DateTime64, also before 1970, with the fraction just below each half hour.
SELECT 'DateTime64, America/New_York';
SELECT s, toUTCTimestamp(toDateTime64(s, 3), 'America/New_York') AS t,
    toUnixTimestamp64Milli(t) = toUnixTimestamp64Milli(toDateTime64(s, 3, 'America/New_York'))
FROM (SELECT concat(formatDateTime(toDateTime64(d, 0) + toIntervalSecond(n * 1800 - 1), '%Y-%m-%d %H:%i:%s'), '.750') AS s
      FROM (SELECT arrayJoin(['1969-10-26', '2026-03-08']) AS d, arrayJoin(range(16)) AS n))
ORDER BY s;

-- Every 15 minutes of 2026: the number of local times converted differently from `toDateTime(s, time_zone)`.
SELECT
    countIf(toUnixTimestamp(toUTCTimestamp(w, 'Europe/Berlin')) != toUnixTimestamp(toDateTime(s, 'Europe/Berlin'))),
    countIf(toUnixTimestamp(toUTCTimestamp(w, 'America/New_York')) != toUnixTimestamp(toDateTime(s, 'America/New_York'))),
    countIf(toUnixTimestamp(toUTCTimestamp(w, 'Australia/Sydney')) != toUnixTimestamp(toDateTime(s, 'Australia/Sydney'))),
    countIf(toUnixTimestamp(toUTCTimestamp(w, 'Australia/Lord_Howe')) != toUnixTimestamp(toDateTime(s, 'Australia/Lord_Howe'))),
    countIf(toUnixTimestamp(toUTCTimestamp(w, 'America/Santiago')) != toUnixTimestamp(toDateTime(s, 'America/Santiago'))),
    countIf(toUnixTimestamp(toUTCTimestamp(w, 'Africa/Casablanca')) != toUnixTimestamp(toDateTime(s, 'Africa/Casablanca')))
FROM (SELECT toDateTime('2026-01-01 00:00:00') + number * 900 AS w, formatDateTime(w, '%Y-%m-%d %H:%i:%s') AS s FROM numbers(35040));
