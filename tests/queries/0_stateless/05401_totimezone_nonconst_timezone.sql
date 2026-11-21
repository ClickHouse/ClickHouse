-- A non-constant time zone argument cannot be honored by toTimeZone(),
-- because the result type has a single time zone. Regression test for #126425.
-- The error under allow_nonconst_timezone_arguments = 0 is kept:
SELECT toTimeZone(toDateTime('2024-01-01 00:00:00', 'UTC'), tz) FROM values('tz String', 'Asia/Tokyo', 'America/New_York') SETTINGS allow_nonconst_timezone_arguments = 0; -- { serverError ILLEGAL_COLUMN }
SELECT toTimeZone(toDateTime('2024-01-01 00:00:00', 'UTC'), tz) FROM values('tz String', 'Asia/Tokyo', 'America/New_York') SETTINGS allow_nonconst_timezone_arguments = 1; -- { serverError ILLEGAL_COLUMN }
SELECT toTimeZone(toDateTime64('2024-01-01 00:00:00.123', 3, 'UTC'), tz) FROM values('tz String', 'Asia/Tokyo', 'America/New_York') SETTINGS allow_nonconst_timezone_arguments = 1; -- { serverError ILLEGAL_COLUMN }
SELECT toTimeZone(toDateTime('2024-01-01 00:00:00', 'UTC'), materialize('Asia/Tokyo')) SETTINGS allow_nonconst_timezone_arguments = 1; -- { serverError ILLEGAL_COLUMN }
-- Constant time zone arguments are unaffected:
SELECT toTimeZone(toDateTime('2024-01-01 00:00:00', 'UTC'), 'Asia/Tokyo');
SELECT toTimeZone(toDateTime64('2024-01-01 00:00:00.123', 3, 'UTC'), 'Asia/Tokyo');
-- A time zone argument that becomes constant only after type resolution cannot be
-- honored either: the resolved result type would carry a fallback time zone
-- (e.g. for an argument coming from a single-row subquery). An empty time zone name
-- selects the default (session) time zone, so it is covered as well:
SELECT toTimeZone(toDateTime('2024-01-01 00:00:00', 'UTC'), tz)
FROM (SELECT toDateTime('2024-01-01 00:00:00', 'UTC') AS dt, '' AS tz)
SETTINGS allow_nonconst_timezone_arguments = 1, session_timezone = 'Asia/Tokyo'; -- { serverError ILLEGAL_COLUMN }
SELECT toTimeZone(toDateTime64('2024-01-01 00:00:00.000', 3, 'UTC'), tz)
FROM (SELECT toDateTime('2024-01-01 00:00:00', 'UTC') AS dt, '' AS tz)
SETTINGS allow_nonconst_timezone_arguments = 1, session_timezone = 'Asia/Tokyo'; -- { serverError ILLEGAL_COLUMN }
-- The new enforcement is versioned: with the `compatibility` setting set to a
-- previous version the legacy behavior (silently ignoring such arguments) is
-- restored, so existing stored expressions keep working:
SET compatibility = '26.10';
SET allow_nonconst_timezone_arguments = 1;
CREATE TABLE tz_legacy (dt DateTime('UTC'), tz String, result_m DateTime MATERIALIZED toTimezone(dt, tz), result_a DateTime ALIAS toTimezone(dt, tz)) ENGINE = Memory;
INSERT INTO tz_legacy (dt, tz) VALUES ('2024-01-01 00:00:00', 'Asia/Tokyo'), ('2024-06-01 12:00:00', 'America/New_York');
SELECT dt, tz, toInt32(result_m), toInt32(result_a) FROM tz_legacy ORDER BY dt;
SET compatibility = '';
-- Without the compatibility setting the read of the ALIAS column (which executes the
-- default expression per row) applies the new enforcement again:
SELECT toInt32(result_a) FROM tz_legacy ORDER BY dt LIMIT 1; -- { serverError ILLEGAL_COLUMN }
-- Stored values of the MATERIALIZED column are unaffected:
SELECT toInt32(result_m) FROM tz_legacy ORDER BY dt;
