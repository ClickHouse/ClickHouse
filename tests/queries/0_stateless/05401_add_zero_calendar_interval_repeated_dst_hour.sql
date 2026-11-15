-- Adding a zero calendar interval returns the same instant, also in the repeated hour after clocks are turned back.

-- The second 02:30 of 2021-10-31 in Europe/Amsterdam.
WITH toTimeZone(toDateTime('2021-10-31 01:30:00', 'UTC'), 'Europe/Amsterdam') AS x
SELECT toTimeZone(x + INTERVAL 0 DAY, 'UTC'),
    (x - INTERVAL 0 DAY, addWeeks(x, 0), addMonths(x, 0), addQuarters(x, 0), addYears(x, 0), subtractYears(x, 0)) = (x, x, x, x, x, x);

WITH toTimeZone(toDateTime64('2021-10-31 01:30:00.250', 3, 'UTC'), 'Europe/Amsterdam') AS x
SELECT toTimeZone(addDays(x, 0), 'UTC'), (addMonths(x, 0), addYears(x, 0)) = (x, x);

-- A non-constant delta: zero keeps the instant, one day keeps the wall-clock time.
WITH toTimeZone(toDateTime('2021-10-31 01:30:00', 'UTC'), 'Europe/Amsterdam') AS x
SELECT number, toTimeZone(x + toIntervalDay(number), 'UTC') FROM numbers(2);

-- The first 02:30 was already kept.
SELECT toTimeZone(addDays(toTimeZone(toDateTime('2021-10-31 00:30:00', 'UTC'), 'Europe/Amsterdam'), 0), 'UTC');

-- Other time zones, including a 30-minute change.
WITH toTimeZone(toDateTime('2021-11-07 06:30:00', 'UTC'), 'America/New_York') AS x
SELECT toTimeZone(addDays(x, 0), 'UTC'), (addMonths(x, 0), addYears(x, 0)) = (x, x);
WITH toTimeZone(toDateTime('2021-04-03 15:15:00', 'UTC'), 'Australia/Lord_Howe') AS x
SELECT toTimeZone(addDays(x, 0), 'UTC'), (addMonths(x, 0), addYears(x, 0)) = (x, x);

-- Outside the lookup table (years after 2299).
WITH toTimeZone(toDateTime64('2400-10-29 01:30:00', 0, 'UTC'), 'Europe/Amsterdam') AS x
SELECT toTimeZone(addDays(x, 0), 'UTC'), (addMonths(x, 0), addYears(x, 0)) = (x, x);

-- Beyond the representable calendar a zero interval is the identity, as for INTERVAL 0 HOUR.
WITH fromUnixTimestamp64Second(toInt64(1000000000000), 'Europe/Amsterdam') AS x
SELECT toUnixTimestamp64Second(x + INTERVAL 0 DAY), toUnixTimestamp64Second(addMonths(x, 0)), toUnixTimestamp64Second(addYears(x, 0));
