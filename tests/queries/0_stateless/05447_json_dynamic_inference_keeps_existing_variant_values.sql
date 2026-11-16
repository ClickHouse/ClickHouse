-- During type inference a value that does not fit an existing variant of a Dynamic as is gets its own type,
-- like a first value would, instead of being saturated, truncated or partly replaced by defaults.

SET session_timezone = 'UTC';

SELECT 'Date';
SELECT id, j.a, dynamicType(j.a) FROM (SELECT id, json::JSON AS j FROM values('id UInt8, json String',
    (1, '{"a": "2024-01-15"}'), (2, '{"a": "1900-01-01"}'), (3, '{"a": "2149-06-07"}'), (4, '{"a": "2299-12-31"}'), (5, '{"a": "2024-02-01"}')))
ORDER BY id;

SELECT 'DateTime';
SELECT id, j.a, dynamicType(j.a) FROM (SELECT id, json::JSON AS j FROM values('id UInt8, json String',
    (1, '{"a": "2024-01-15 12:30:00"}'), (2, '{"a": "1900-01-01 00:00:00"}'), (3, '{"a": "2200-01-01 00:00:00"}'),
    (4, '{"a": "2024-01-15 12:30:00.123"}'), (5, '{"a": "2024-01-15 13:00:00"}')))
ORDER BY id;

SELECT 'Array(Nullable(Date))';
SELECT id, j.a, dynamicType(j.a) FROM (SELECT id, json::JSON AS j FROM values('id UInt8, json String',
    (1, '{"a": ["2024-01-15"]}'), (2, '{"a": ["1900-01-01"]}')))
ORDER BY id;

SELECT 'Shared data';
SELECT id, j.a, JSONSharedDataPathsWithTypes(j) FROM (SELECT id, json::JSON(max_dynamic_paths = 0) AS j FROM values('id UInt8, json String',
    (1, '{"a": "2024-01-15"}'), (2, '{"a": "1900-01-01"}'), (3, '{"a": "2024-01-15 12:30:00.123"}')))
ORDER BY id;

SELECT 'JSONExtract into Dynamic';
SELECT id, d, dynamicType(d) FROM (SELECT id, JSONExtract(json, 'a', 'Dynamic') AS d FROM values('id UInt8, json String',
    (1, '{"a": "2024-01-15"}'), (2, '{"a": "1900-01-01"}'), (3, '{"a": [1, 2]}'), (4, '{"a": [1, "x"]}')))
ORDER BY id;
