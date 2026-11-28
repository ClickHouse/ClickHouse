DROP DICTIONARY IF EXISTS squality_nullable_nan_dict;
DROP TABLE IF EXISTS squality_nullable_nan_source;

CREATE TABLE squality_nullable_nan_source
(
    k Nullable(Float64),
    part UInt8,
    v String
) ENGINE = Memory;
INSERT INTO squality_nullable_nan_source VALUES
    (reinterpretAsFloat64(toUInt64(9221120237041090560)), 1, 'nan-key');

CREATE DICTIONARY squality_nullable_nan_dict
(
    k Nullable(Float64),
    part UInt8,
    v String DEFAULT 'missing'
)
PRIMARY KEY k, part
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' PASSWORD '' DB currentDatabase()
    TABLE 'squality_nullable_nan_source'))
LAYOUT(COMPLEX_KEY_DIRECT())
SETTINGS(in_null_as_unknown = 1);

SELECT dictHas('squality_nullable_nan_dict',
    (toNullable(reinterpretAsFloat64(toUInt64(9221120237041090560))), toUInt8(1)));
SELECT dictHas('squality_nullable_nan_dict',
    (toNullable(reinterpretAsFloat64(toUInt64(9221120237041090561))), toUInt8(1)));
SELECT dictGet('squality_nullable_nan_dict', 'v',
    (toNullable(reinterpretAsFloat64(toUInt64(9221120237041090560))), toUInt8(1)));

DROP DICTIONARY squality_nullable_nan_dict;
DROP TABLE squality_nullable_nan_source;
