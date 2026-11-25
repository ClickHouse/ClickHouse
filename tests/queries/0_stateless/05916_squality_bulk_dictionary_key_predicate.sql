DROP DICTIONARY IF EXISTS squality_bulk_key_dict;
DROP TABLE IF EXISTS squality_bulk_key_source;

CREATE TABLE squality_bulk_key_source
(
    k1 UInt64,
    k2 UInt64,
    v UInt8
) ENGINE = Memory;
INSERT INTO squality_bulk_key_source
SELECT number, number, toUInt8(1) FROM numbers(6000);

CREATE DICTIONARY squality_bulk_key_dict
(
    k1 UInt64,
    k2 UInt64,
    v UInt8 DEFAULT 0
)
PRIMARY KEY k1, k2
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' PASSWORD '' DB currentDatabase()
    TABLE 'squality_bulk_key_source'))
LAYOUT(COMPLEX_KEY_DIRECT())
SETTINGS(in_null_as_unknown = 0);

SELECT sum(dictHas('squality_bulk_key_dict', (number, number)))
FROM numbers(6000)
SETTINGS max_block_size = 10000, max_threads = 1;

DROP DICTIONARY squality_bulk_key_dict;
DROP TABLE squality_bulk_key_source;
