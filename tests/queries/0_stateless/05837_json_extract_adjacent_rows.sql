-- The rows of a `String` column are stored back to back, and JSON functions parse each row in place
-- when enough bytes follow it. A row must not be completed by the bytes of the next row.
-- The last row is longer than the padding simdjson needs, so every row before it is parsed in place.

SELECT s, isValidJSON(s), JSONExtractRaw(s)
FROM
(
    SELECT arrayJoin([
        '123', '456',
        'tru', 'e',
        'nul', 'l',
        'fals', 'e',
        '"ab', 'c"',
        '"ab"',
        '-', '1',
        '0', '7',
        '[1,2', ']',
        '{"a":"b"', '}',
        '{"a":1}',
        '',
        '{"padding":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}'
    ]) AS s
)
SETTINGS allow_simdjson = 1;
