DROP DICTIONARY IF EXISTS squality_nullable_key_direct;
DROP DICTIONARY IF EXISTS squality_nullable_key_cache;
DROP TABLE IF EXISTS squality_nullable_key_source;

CREATE TABLE squality_nullable_key_source
(
    k Nullable(UInt64),
    part UInt8,
    v String
) ENGINE = Memory;
INSERT INTO squality_nullable_key_source VALUES (NULL, 1, 'present');

CREATE DICTIONARY squality_nullable_key_direct
(
    k Nullable(UInt64),
    part UInt8,
    v String DEFAULT 'missing'
)
PRIMARY KEY k, part
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' PASSWORD '' DB currentDatabase()
    TABLE 'squality_nullable_key_source'))
LAYOUT(COMPLEX_KEY_DIRECT())
SETTINGS(transform_null_in = 1, in_null_as_unknown = 0);

CREATE DICTIONARY squality_nullable_key_cache
(
    k Nullable(UInt64),
    part UInt8,
    v String DEFAULT 'missing'
)
PRIMARY KEY k, part
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' PASSWORD '' DB currentDatabase()
    TABLE 'squality_nullable_key_source'))
LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 16))
LIFETIME(30)
SETTINGS(transform_null_in = 1, in_null_as_unknown = 0);

SELECT dictHas('squality_nullable_key_direct', (CAST(NULL AS Nullable(UInt64)), toUInt8(1))),
    dictGet('squality_nullable_key_direct', 'v', (CAST(NULL AS Nullable(UInt64)), toUInt8(1)));
SELECT dictHas('squality_nullable_key_cache', (CAST(NULL AS Nullable(UInt64)), toUInt8(1))),
    dictGet('squality_nullable_key_cache', 'v', (CAST(NULL AS Nullable(UInt64)), toUInt8(1)));

DROP DICTIONARY squality_nullable_key_direct;
DROP DICTIONARY squality_nullable_key_cache;
DROP TABLE squality_nullable_key_source;
