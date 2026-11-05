-- toDateTimeOrNull, toDateTimeOrZero, toDateTime64OrNull and toDateTime64OrZero follow cast_string_to_date_time_mode like toDateTime.

SET session_timezone = 'UTC';

SELECT 'best_effort';
SELECT s, toDateTime(s), toDateTimeOrNull(s), toDateTimeOrZero(s), toDateTime64OrNull(s, 3), toDateTime64OrZero(s, 3),
    toDateTimeOrNull(s) = toDateTime(s), toDateTime64OrNull(s, 3) = toDateTime64(s, 3)
FROM (SELECT arrayJoin(['2024-01-05 10:00:00', '20240105', '2024-01-05T10:00:00Z', '2024-01-05T10:00:00.123Z', 'Jan 5 2024 10:00:00', '02/01/2024']) AS s)
SETTINGS cast_string_to_date_time_mode = 'best_effort';
SELECT s, toDateTimeOrNull(s), toDateTimeOrZero(s), toDateTime64OrNull(s, 3), toDateTime64OrZero(s, 3)
FROM (SELECT arrayJoin(['abc', '2024-13-45', '']) AS s)
SETTINGS cast_string_to_date_time_mode = 'best_effort';
SELECT toDateTimeOrNull(materialize('20240105')), toDateTimeOrNull(toFixedString('20240105', 8)), toDateTime64OrZero(materialize('2024-01-05T10:00:00Z'), 0, 'Asia/Shanghai')
SETTINGS cast_string_to_date_time_mode = 'best_effort';

SELECT 'best_effort_us';
SELECT s, toDateTime(s), toDateTimeOrNull(s), toDateTimeOrZero(s), toDateTime64OrNull(s, 3), toDateTime64OrZero(s, 3),
    toDateTimeOrNull(s) = toDateTime(s), toDateTime64OrNull(s, 3) = toDateTime64(s, 3)
FROM (SELECT arrayJoin(['2024-01-05 10:00:00', '20240105', '2024-01-05T10:00:00Z', '02/01/2024']) AS s)
SETTINGS cast_string_to_date_time_mode = 'best_effort_us';
SELECT s, toDateTimeOrNull(s), toDateTimeOrZero(s), toDateTime64OrNull(s, 3), toDateTime64OrZero(s, 3)
FROM (SELECT arrayJoin(['abc', '2024-13-45', '']) AS s)
SETTINGS cast_string_to_date_time_mode = 'best_effort_us';

SELECT 'basic';
SELECT s, toDateTime(s), toDateTimeOrNull(s), toDateTimeOrZero(s), toDateTime64OrNull(s, 3), toDateTime64OrZero(s, 3),
    toDateTimeOrNull(s) = toDateTime(s), toDateTime64OrNull(s, 3) = toDateTime64(s, 3)
FROM (SELECT arrayJoin(['2024-01-05 10:00:00', '20240105', '1700000000']) AS s)
SETTINGS cast_string_to_date_time_mode = 'basic';
SELECT s, toDateTimeOrNull(s), toDateTimeOrZero(s), toDateTime64OrNull(s, 3), toDateTime64OrZero(s, 3)
FROM (SELECT arrayJoin(['2024-01-05T10:00:00Z', 'Jan 5 2024 10:00:00', 'abc']) AS s)
SETTINGS cast_string_to_date_time_mode = 'basic';
