-- Before the epoch, toStartOfInterval with a minute interval must round to the same boundary as
-- toStartOfFiveMinutes, toStartOfTenMinutes and toStartOfFifteenMinutes: relative to the local midnight.
-- That differs from rounding the Unix timestamp when the UTC offset is not a multiple of the interval.
SET enable_extended_results_for_datetime_functions = 1;
SET to_start_of_interval_preserves_argument_type = 1;

SELECT 'Europe/Warsaw was at +01:24 until 1915';
SELECT toStartOfInterval(toDateTime64('1910-06-15 12:07:00', 0, 'Europe/Warsaw'), INTERVAL 5 MINUTE);
SELECT toStartOfInterval(toDateTime64('1910-06-15 12:07:00', 0, 'Europe/Warsaw'), INTERVAL 10 MINUTE);
SELECT toStartOfInterval(toDateTime64('1910-06-15 12:07:00', 0, 'Europe/Warsaw'), INTERVAL 15 MINUTE);

SELECT 'Indian/Maldives was at +04:54 until 1960';
SELECT toStartOfInterval(toDateTime64('1950-01-01 12:07:00', 0, 'Indian/Maldives'), INTERVAL 5 MINUTE);
SELECT toStartOfInterval(toDateTime64('1950-01-01 12:07:00', 0, 'Indian/Maldives'), INTERVAL 10 MINUTE);
SELECT toStartOfInterval(toDateTime64('1950-01-01 12:07:00', 0, 'Indian/Maldives'), INTERVAL 15 MINUTE);

SELECT 'the dedicated functions agree before the epoch';
SELECT countIf(toStartOfInterval(t, INTERVAL 5 MINUTE) != toStartOfFiveMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 10 MINUTE) != toStartOfTenMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 15 MINUTE) != toStartOfFifteenMinutes(t))
FROM (SELECT toDateTime64('1901-01-01 00:00:00', 0, 'Europe/Warsaw') + INTERVAL 4999 * number SECOND AS t FROM numbers(20000));
SELECT countIf(toStartOfInterval(t, INTERVAL 5 MINUTE) != toStartOfFiveMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 10 MINUTE) != toStartOfTenMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 15 MINUTE) != toStartOfFifteenMinutes(t))
FROM (SELECT toDateTime64('1945-01-01 00:00:00', 0, 'Indian/Maldives') + INTERVAL 4999 * number SECOND AS t FROM numbers(20000));
SELECT countIf(toStartOfInterval(t, INTERVAL 5 MINUTE) != toStartOfFiveMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 10 MINUTE) != toStartOfTenMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 15 MINUTE) != toStartOfFifteenMinutes(t))
FROM (SELECT toDateTime64('1960-01-01 00:00:00', 0, 'Pacific/Chatham') + INTERVAL 4999 * number SECOND AS t FROM numbers(20000));
SELECT countIf(toStartOfInterval(t, INTERVAL 5 MINUTE) != toStartOfFiveMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 10 MINUTE) != toStartOfTenMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 15 MINUTE) != toStartOfFifteenMinutes(t))
FROM (SELECT toDateTime64('1960-01-01 00:00:00', 0, 'UTC') + INTERVAL 4999 * number SECOND AS t FROM numbers(20000));

SELECT 'an interval that does not divide an hour keeps the grid counted from the epoch';
SELECT toStartOfInterval(toDateTime64('1969-12-31 23:59:00', 0, 'UTC'), INTERVAL 7 MINUTE);
SELECT toStartOfInterval(toDateTime64('1970-01-01 00:59:00', 0, 'Europe/Berlin'), INTERVAL 7 MINUTE);
SELECT toStartOfInterval(toDateTime64('1969-12-31 12:00:00', 0, 'UTC'), INTERVAL 2880 MINUTE) = toStartOfInterval(toDateTime64('1969-12-31 12:00:00', 0, 'UTC'), INTERVAL 2 DAY);
