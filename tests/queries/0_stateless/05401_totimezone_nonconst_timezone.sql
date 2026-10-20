-- A non-constant time zone argument cannot be honoured by toTimeZone(),
-- because the result type has a single time zone. Regression test for #126425.
-- The error under allow_nonconst_timezone_arguments = 0 is kept:
SELECT toTimeZone(toDateTime('2024-01-01 00:00:00', 'UTC'), tz) FROM values('tz String', 'Asia/Tokyo', 'America/New_York') SETTINGS allow_nonconst_timezone_arguments = 0; -- { serverError ILLEGAL_COLUMN }
SELECT toTimeZone(toDateTime('2024-01-01 00:00:00', 'UTC'), tz) FROM values('tz String', 'Asia/Tokyo', 'America/New_York') SETTINGS allow_nonconst_timezone_arguments = 1; -- { serverError ILLEGAL_COLUMN }
SELECT toTimeZone(toDateTime64('2024-01-01 00:00:00.123', 3, 'UTC'), tz) FROM values('tz String', 'Asia/Tokyo', 'America/New_York') SETTINGS allow_nonconst_timezone_arguments = 1; -- { serverError ILLEGAL_COLUMN }
SELECT toTimeZone(toDateTime('2024-01-01 00:00:00', 'UTC'), materialize('Asia/Tokyo')) SETTINGS allow_nonconst_timezone_arguments = 1; -- { serverError ILLEGAL_COLUMN }
-- Constant time zone arguments are unaffected:
SELECT toTimeZone(toDateTime('2024-01-01 00:00:00', 'UTC'), 'Asia/Tokyo');
SELECT toTimeZone(toDateTime64('2024-01-01 00:00:00.123', 3, 'UTC'), 'Asia/Tokyo');
