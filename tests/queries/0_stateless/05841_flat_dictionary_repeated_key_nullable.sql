-- FLAT keeps the last row of a repeated key, also when an earlier or a later row of the key is NULL.
CREATE DICTIONARY d_flat_repeated_nullable
(
    k UInt64,
    s Nullable(String),
    j Nullable(JSON),
    p Nullable(UInt64) HIERARCHICAL
)
PRIMARY KEY k
SOURCE(CLICKHOUSE(QUERY 'SELECT k, s, j, p FROM values(''k UInt64, s Nullable(String), j Nullable(JSON), p Nullable(UInt64), o UInt64'',
    (0, NULL, NULL, NULL, 1), (0, ''z'', ''{"v":"z"}'', 5, 2),
    (1, NULL, NULL, NULL, 1), (1, ''b'', ''{"v":"b"}'', 5, 2),
    (2, ''a'', ''{"v":"a"}'', 5, 1), (2, NULL, NULL, NULL, 2),
    (3, ''a'', ''{"v":"a"}'', 6, 1), (3, ''b'', ''{"v":"b"}'', 5, 2)) ORDER BY o'))
LIFETIME(0)
LAYOUT(FLAT());

SELECT number AS k, dictGet('d_flat_repeated_nullable', 's', k), toString(dictGet('d_flat_repeated_nullable', 'j', k)),
       dictGetHierarchy('d_flat_repeated_nullable', k)
FROM numbers(4);

SELECT dictGetChildren('d_flat_repeated_nullable', 5);

DROP DICTIONARY d_flat_repeated_nullable;
