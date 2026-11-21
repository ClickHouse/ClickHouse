-- A day followed by a month name ('08-Nov-2024') is not ambiguous, so the US variant must parse it
-- the same way as parseDateTimeBestEffort. It expected a numeric day after the month and threw.

SELECT parseDateTimeBestEffortUS('08-Nov-2024 10:53:45', 'UTC');
SELECT parseDateTimeBestEffortUS('8-Nov-2024', 'UTC');
SELECT parseDateTimeBestEffortUS('08/Nov/2024', 'UTC');
SELECT parseDateTimeBestEffortUS('08.Nov.2024', 'UTC');
SELECT parseDateTimeBestEffortUS('8-November-2024', 'UTC');

SELECT parseDateTimeBestEffortUSOrNull('8-Nov-2024', 'UTC');
SELECT parseDateTimeBestEffortUSOrZero('8-Nov-2024', 'UTC');
SELECT parseDateTime64BestEffortUS('08-Nov-2024 10:53:45.123', 3, 'UTC');
SELECT parseDateTime64BestEffortUSOrNull('08-Nov-2024 10:53:45.123', 3, 'UTC');
SELECT * FROM format(CSV, 'd DateTime(\'UTC\')', '08-Nov-2024 10:53:45') SETTINGS date_time_input_format = 'best_effort_us';

-- A word that is not a month name is still rejected.
SELECT parseDateTimeBestEffortUSOrNull('08-Foo-2024', 'UTC');
SELECT parseDateTimeBestEffortUS('08-Foo-2024', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- Numeric day and month are ambiguous and keep their order: month first for the US variant, day first otherwise.
SELECT parseDateTimeBestEffortUS('02/03/2024', 'UTC');
SELECT parseDateTimeBestEffort('02/03/2024', 'UTC');
