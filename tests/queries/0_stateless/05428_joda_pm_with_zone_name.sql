-- A PM time parsed by a Joda format with a time zone name (`z`) got 12 hours added twice:
-- `jodaTimezone` builds the date time once to resolve the zone, and that call changed the parsed hour.

SELECT s, parseDateTimeInJodaSyntax(s, 'yyyy-MM-dd hh:mm a z', 'UTC')
FROM (SELECT arrayJoin(['2024-01-01 01:30 AM UTC', '2024-01-01 01:30 PM UTC', '2024-01-01 12:00 PM UTC', '2024-01-01 12:30 AM UTC']) AS s)
ORDER BY s;
SELECT parseDateTimeInJodaSyntax('2024-01-01 01:30 PM UTC', 'yyyy-MM-dd KK:mm a z', 'UTC');
SELECT parseDateTimeInJodaSyntax('2024-01-01 01:30:00 PM EST', 'yyyy-MM-dd hh:mm:ss a z', 'UTC');
SELECT parseDateTimeInJodaSyntax('2024-01-01 01:30:00 PM America/New_York', 'yyyy-MM-dd hh:mm:ss a z', 'UTC');
SELECT parseDateTime64InJodaSyntax('2024-01-01 01:30:00.123 PM UTC', 'yyyy-MM-dd hh:mm:ss.SSS a z', 'UTC');
SELECT parseDateTimeInJodaSyntaxOrNull('2024-01-01 11:59 PM UTC', 'yyyy-MM-dd hh:mm a z', 'UTC');
