-- Rounding to the start of an hour must land on a wall clock hour, also on a day when the UTC offset
-- changes by a fraction of an hour. Australia/Lord_Howe shifts by 30 minutes twice a year.

SELECT 'clocks go back (2024-04-07, 02:00 -> 01:30)';
SELECT toStartOfHour(toDateTime('2024-04-07 01:10:00', 'Australia/Lord_Howe'));
SELECT toStartOfHour(toDateTime('2024-04-07 03:10:00', 'Australia/Lord_Howe'));
SELECT toStartOfHour(toDateTime('2024-04-07 12:58:30', 'Australia/Lord_Howe'));
SELECT toStartOfHour(toDateTime('2024-04-07 23:59:59', 'Australia/Lord_Howe'));
SELECT toStartOfHour(toDateTime('2024-04-08 05:20:00', 'Australia/Lord_Howe'));

SELECT 'clocks go forward (2024-10-06, 02:00 -> 02:30)';
SELECT toStartOfHour(toDateTime('2024-10-06 01:10:00', 'Australia/Lord_Howe'));
SELECT toStartOfHour(toDateTime('2024-10-06 06:00:00', 'Australia/Lord_Howe'));
SELECT toStartOfHour(toDateTime('2024-10-06 06:45:00', 'Australia/Lord_Howe'));
SELECT toStartOfHour(toDateTime('2024-10-06 23:59:59', 'Australia/Lord_Howe'));

SELECT 'the other ways to round to an hour agree';
SELECT dateTrunc('hour', toDateTime('2024-10-06 06:00:00', 'Australia/Lord_Howe'));
SELECT toStartOfInterval(toDateTime('2024-04-07 03:10:00', 'Australia/Lord_Howe'), INTERVAL 1 HOUR);
SELECT toStartOfInterval(toDateTime('2024-04-07 03:10:00', 'Australia/Lord_Howe'), INTERVAL 2 HOUR);
SELECT toStartOfHour(toDateTime64('2024-04-07 12:58:30.123', 3, 'Australia/Lord_Howe'));

SELECT 'every time after the shift keeps its hour and lands on :00';
SELECT countIf(toMinute(toStartOfHour(t)) != 0 OR toHour(toStartOfHour(t)) != toHour(t) OR toStartOfHour(t) > t OR t - toStartOfHour(t) >= 3600)
FROM (SELECT toDateTime('2024-04-07 03:00:00', 'Australia/Lord_Howe') + INTERVAL 7 * number MINUTE AS t FROM numbers(120));
SELECT countIf(toMinute(toStartOfHour(t)) != 0 OR toHour(toStartOfHour(t)) != toHour(t) OR toStartOfHour(t) > t OR t - toStartOfHour(t) >= 3600)
FROM (SELECT toDateTime('2024-10-06 03:00:00', 'Australia/Lord_Howe') + INTERVAL 7 * number MINUTE AS t FROM numbers(120));

SELECT 'zones whose shift is a whole number of hours are unchanged';
SELECT toStartOfHour(toDateTime('2024-03-31 03:40:00', 'Europe/Berlin'));
SELECT toStartOfHour(toDateTime('2024-10-27 03:40:00', 'Europe/Berlin'));
SELECT toStartOfHour(toDateTime('2024-04-07 12:58:30', 'Australia/Adelaide'));
SELECT toStartOfHour(toDateTime('2024-04-07 12:58:30', 'Asia/Kolkata'));
