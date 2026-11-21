-- toDaysSinceYearZero of a DateTime is the number of the local calendar day, like it is for Date and DateTime64.

SELECT 'the time zone of the column';
SELECT toDaysSinceYearZero(toDateTime('2024-01-01 23:00:00', 'America/Los_Angeles'));
SELECT toDaysSinceYearZero(toDateTime('2024-01-01 01:00:00', 'Asia/Kolkata'));
SELECT toDaysSinceYearZero(toDateTime('2024-01-01 12:00:00', 'UTC'));

SELECT 'the time zone argument';
SELECT toDaysSinceYearZero(toDateTime('2024-01-02 03:00:00', 'UTC'), 'America/Los_Angeles');
SELECT TO_DAYS(toDateTime('2024-01-02 03:00:00', 'UTC'), 'America/Los_Angeles');

SELECT 'it agrees with Date and DateTime64 around every midnight';
SELECT countIf(toDaysSinceYearZero(t) != toDaysSinceYearZero(toDate(t)) OR toDaysSinceYearZero(t) != toDaysSinceYearZero(toDateTime64(t, 0, 'America/Los_Angeles')))
FROM (SELECT toTimeZone(toDateTime('2024-01-01 00:00:00', 'UTC') + number * 1800, 'America/Los_Angeles') AS t FROM numbers(2000));
SELECT countIf(toDaysSinceYearZero(t) != toDaysSinceYearZero(toDate(t)) OR toDaysSinceYearZero(t) != toDaysSinceYearZero(toDateTime64(t, 0, 'Pacific/Auckland')))
FROM (SELECT toTimeZone(toDateTime('2024-01-01 00:00:00', 'UTC') + number * 1800, 'Pacific/Auckland') AS t FROM numbers(2000));

SELECT 'the local day at the epoch is the day before it west of UTC';
SELECT toDaysSinceYearZero(toDateTime(0, 'America/Los_Angeles'));
SELECT toDaysSinceYearZero(toDateTime64(0, 0, 'America/Los_Angeles'));
SELECT toDaysSinceYearZero(toDateTime(0, 'UTC'), 'America/Los_Angeles');
SELECT TO_DAYS(toDateTime(0, 'UTC'), 'America/Los_Angeles');
