-- A month name followed by a dash or slash ('Nov-08-2024') must be parsed like 'Nov 08 2024'.
-- The '-08' was read as a time zone offset, '-2024' as a second one, and the year was taken from today.

SELECT parseDateTimeBestEffort('Nov-08-2024', 'UTC');
SELECT parseDateTimeBestEffort('November-08-2024', 'UTC');
SELECT parseDateTimeBestEffort('Nov-8-2024', 'UTC');
SELECT parseDateTimeBestEffort('Nov/08/2024', 'UTC');
SELECT parseDateTimeBestEffort('Nov-08-24', 'UTC');
SELECT parseDateTimeBestEffort('Fri, Nov-08-2024', 'UTC');
SELECT parseDateTimeBestEffort('Nov-08-2024 10:00:00', 'UTC');
SELECT parseDateTimeBestEffort('Nov-08-2024 10:00:00 -08:00', 'UTC');
SELECT parseDateTimeBestEffortUS('Nov-08-2024', 'UTC');
SELECT parseDateTime64BestEffort('Nov-08-2024 10:00:00.123', 3, 'UTC');
SELECT parseDateTimeBestEffort('Nov-08', 'UTC') = parseDateTimeBestEffort('Nov 08', 'UTC');
SELECT toTypeName(c1) FROM format(CSV, 'Nov-08-2024') SETTINGS date_time_input_format = 'best_effort';

-- 'Nov-YYYY' is parsed like 'Nov YYYY' (day 1), 'Nov-DD' like 'Nov DD'. Without a day, schema inference does not take it for DateTime.
SELECT parseDateTimeBestEffort('Nov-2024', 'UTC');
SELECT parseDateTimeBestEffort('Nov-24', 'UTC') = parseDateTimeBestEffort('Nov 24', 'UTC');
SELECT toTypeName(c1) FROM format(CSV, 'Nov-2024') SETTINGS date_time_input_format = 'best_effort';

-- Malformed day or year is rejected.
SELECT parseDateTimeBestEffortOrNull('Nov-123-2024', 'UTC');
SELECT parseDateTimeBestEffortOrNull('Nov-08-202', 'UTC');
SELECT parseDateTimeBestEffortOrNull('Nov-32-2024', 'UTC');
SELECT parseDateTimeBestEffortOrNull('Nov-31-2024', 'UTC');
SELECT parseDateTimeBestEffortOrNull('Feb-30-2024', 'UTC');
SELECT parseDateTimeBestEffortOrNull('Feb-29-2023', 'UTC');
SELECT parseDateTimeBestEffort('Feb-29-2024', 'UTC');

-- Only '-' and '/' are handled after a month name; a '.' is not, and the value is not parsed, as before.
SELECT parseDateTimeBestEffortOrNull('Nov.08.2024', 'UTC');

-- Forms that already worked, including real time zone offsets, are not affected.
SELECT parseDateTimeBestEffort('2024-11-08 10:00:00-08', 'UTC');
SELECT parseDateTimeBestEffort('2024-11-08T10:00:00-08:00', 'UTC');
SELECT parseDateTimeBestEffort('Fri, 08 Nov 2024 10:00:00 -0800', 'UTC');
SELECT parseDateTimeBestEffort('08 Nov 2024 10:00 -0800', 'UTC');
SELECT parseDateTimeBestEffort('Nov 08 2024 10:00:00 -08:00', 'UTC');
SELECT parseDateTimeBestEffort('Nov 08 2024 10:00:00-08', 'UTC');
SELECT parseDateTimeBestEffort('08-Nov-2024', 'UTC');
SELECT parseDateTimeBestEffort('Nov 2024', 'UTC');
