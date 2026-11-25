DROP DICTIONARY IF EXISTS squality_dict_signed_zero;
DROP TABLE IF EXISTS squality_dict_signed_zero_source;

CREATE TABLE squality_dict_signed_zero_source (k Float64, part UInt8, v String) ENGINE = Memory;
INSERT INTO squality_dict_signed_zero_source VALUES
    (reinterpretAsFloat64(toUInt64(9223372036854775808)), 1, 'negative zero'),
    (reinterpretAsFloat64(toUInt64(9221120237041090560)), 1, 'nan');

CREATE DICTIONARY squality_dict_signed_zero
(
    k Float64,
    part UInt8,
    v String DEFAULT 'missing'
)
PRIMARY KEY k, part
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' PASSWORD '' DB currentDatabase()
    TABLE 'squality_dict_signed_zero_source'))
LAYOUT(COMPLEX_KEY_DIRECT())
SETTINGS(in_null_as_unknown = 1);

SELECT dictHas('squality_dict_signed_zero', (k, toUInt8(1)))
FROM (SELECT arrayJoin([toFloat64(0), reinterpretAsFloat64(toUInt64(9223372036854775808))]) AS k);
SELECT dictHas('squality_dict_signed_zero',
    (reinterpretAsFloat64(toUInt64(9221120237041090560)), toUInt8(1)));
SELECT dictHas('squality_dict_signed_zero',
    (reinterpretAsFloat64(toUInt64(9221120237041090561)), toUInt8(1)));
SELECT dictGet('squality_dict_signed_zero', 'v',
    (reinterpretAsFloat64(toUInt64(9221120237041090560)), toUInt8(1)));

DROP DICTIONARY squality_dict_signed_zero;
DROP TABLE squality_dict_signed_zero_source;
