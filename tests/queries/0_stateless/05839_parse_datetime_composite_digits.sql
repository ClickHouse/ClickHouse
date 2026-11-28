SELECT parseDateTimeOrNull('2024-10-06', '%F', 'UTC'), parseDateTimeOrNull('10/06/24', '%D', 'UTC');
SELECT parseDateTimeOrNull('2024-0:-06', '%F', 'UTC'), parseDateTimeOrNull('202:-11-06', '%F', 'UTC');
SELECT parseDateTimeOrNull('0:/06/24', '%D', 'UTC'), parseDateTimeOrNull('11/06/2:', '%D', 'UTC');
SELECT parseDateTime64OrNull('2024-0:-06', '%F', 'UTC'), parseDateTime64OrNull('11/06/2:', '%D', 'UTC');
