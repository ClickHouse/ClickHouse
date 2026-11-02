-- A string like "1" or "31" is inferred as String on its own, so it must not be parsed into an existing
-- DateTime or DateTime64 variant of Dynamic either (it would get the current year). Issue #76707.

SET session_timezone = 'UTC';
SET cast_string_to_date_time_mode = 'best_effort', input_format_json_try_infer_numbers_from_strings = 0;

SELECT 'JSONExtract, DateTime first';
SELECT arrayMap(x -> dynamicType(x), l), l FROM (SELECT JSONExtract('{"l": ["2024-12-22 16:18:16", "1", "31", "32"]}', 'l', 'Array(Dynamic)') AS l);

SELECT 'JSONExtract, DateTime64 first';
SELECT arrayMap(x -> dynamicType(x), l), l FROM (SELECT JSONExtract('{"l": ["2024-12-22 16:18:16.123", "1", "1.5"]}', 'l', 'Array(Dynamic)') AS l);

SELECT 'JSON type';
SELECT id, j.p, dynamicType(j.p) FROM (SELECT id, json::JSON AS j FROM values('id UInt8, json String',
    (1, '{"p": "2024-01-01 00:00:00"}'), (2, '{"p": "1"}'), (3, '{"p": "1.5"}'), (4, '{"p": "2024-01-02 03:04:05"}')))
ORDER BY id;

SELECT 'JSON type, array';
SELECT id, j.p, dynamicType(j.p) FROM (SELECT id, json::JSON AS j FROM values('id UInt8, json String',
    (1, '{"p": ["2024-01-01 00:00:00"]}'), (2, '{"p": ["1"]}')))
ORDER BY id;

SELECT 'Date and date time strings still go to an existing DateTime';
SELECT arrayMap(x -> dynamicType(x), l), l FROM (SELECT JSONExtract('{"l": ["2024-12-22 16:18:16", "32", "2024-12-23 10:00:00", "2024-12-23T10:00:00Z", "2024-12-23"]}', 'l', 'Array(Dynamic)') AS l);

SELECT 'Date first';
SELECT arrayMap(x -> dynamicType(x), l), l FROM (SELECT JSONExtract('{"l": ["2024-12-22", "1", "31"]}', 'l', 'Array(Dynamic)') AS l);

SELECT 'Reversed order';
SELECT arrayMap(x -> dynamicType(x), l), l FROM (SELECT JSONExtract('{"l": ["1", "31", "32", "2024-12-22 16:18:16"]}', 'l', 'Array(Dynamic)') AS l);
SELECT arrayMap(x -> dynamicType(x), l), l FROM (SELECT JSONExtract('{"l": ["1", "1.5", "2024-12-22 16:18:16.123"]}', 'l', 'Array(Dynamic)') AS l);
