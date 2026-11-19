SET to_start_of_interval_preserves_argument_type = 1;

-- A `DateTime` rounded to a calendar boundary before the epoch is clamped to the timestamp zero,
-- and never exceeds the argument, also in a time zone with a negative offset.
SELECT
    t,
    countIf(r > t) AS exceeding,
    min(r)
FROM
(
    SELECT
        t,
        arrayJoin([
            toUnixTimestamp(toStartOfInterval(toDateTime(t, 'America/New_York'), INTERVAL 1 DAY)),
            toUnixTimestamp(toStartOfInterval(toDateTime(t, 'America/New_York'), INTERVAL 2 DAY)),
            toUnixTimestamp(toStartOfInterval(toDateTime(t, 'America/New_York'), INTERVAL 1 WEEK)),
            toUnixTimestamp(toStartOfInterval(toDateTime(t, 'America/New_York'), INTERVAL 3 WEEK)),
            toUnixTimestamp(toStartOfInterval(toDateTime(t, 'America/New_York'), INTERVAL 1 MONTH)),
            toUnixTimestamp(toStartOfInterval(toDateTime(t, 'America/New_York'), INTERVAL 2 MONTH)),
            toUnixTimestamp(toStartOfInterval(toDateTime(t, 'America/New_York'), INTERVAL 1 QUARTER)),
            toUnixTimestamp(toStartOfInterval(toDateTime(t, 'America/New_York'), INTERVAL 1 YEAR))]) AS r
    FROM (SELECT arrayJoin([0, 1, 3600, 17999, 18000, 86400, 864000])::UInt32 AS t)
)
GROUP BY t
ORDER BY t;

-- A subsecond unit finer than the scale of a `DateTime64` gives the scale of the unit, so that the bucket boundary is
-- representable and rounding it again keeps it.
WITH
    toDateTime64('2023-01-01 00:00:02', 0, 'UTC') AS t,
    toStartOfInterval(t, INTERVAL 1500 MILLISECOND) AS r
SELECT r, toStartOfInterval(r, INTERVAL 1500 MILLISECOND), toTypeName(r);

SELECT toTypeName(toStartOfInterval(toDateTime64('2023-01-01 00:00:02', 2, 'UTC'), INTERVAL 1 MICROSECOND));
SELECT toTypeName(toStartOfInterval(toDateTime64('2023-01-01 00:00:02', 6, 'UTC'), INTERVAL 1 MILLISECOND));
