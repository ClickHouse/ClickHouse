DROP DICTIONARY IF EXISTS squality_float_bulk_dict;
DROP TABLE IF EXISTS squality_float_bulk_source;

CREATE TABLE squality_float_bulk_source
(
    k Float64,
    part UInt64,
    v UInt8
) ENGINE = Memory;
INSERT INTO squality_float_bulk_source
SELECT toFloat64(0), number, toUInt8(1) FROM numbers(6000);

CREATE DICTIONARY squality_float_bulk_dict
(
    k Float64,
    part UInt64,
    v UInt8 DEFAULT 0
)
PRIMARY KEY k, part
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' PASSWORD '' DB currentDatabase()
    TABLE 'squality_float_bulk_source'))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT sum(dictHas('squality_float_bulk_dict', (toFloat64(0), number)))
FROM numbers(6000)
SETTINGS max_block_size = 10000, max_threads = 1;

DROP DICTIONARY squality_float_bulk_dict;
DROP TABLE squality_float_bulk_source;
