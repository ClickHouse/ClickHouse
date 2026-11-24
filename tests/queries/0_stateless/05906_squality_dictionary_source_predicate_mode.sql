DROP DICTIONARY IF EXISTS squality_dict_source_direct;
DROP DICTIONARY IF EXISTS squality_dict_source_hashed;
DROP TABLE IF EXISTS squality_dict_source_rows;

CREATE TABLE squality_dict_source_rows
(
    key_column UInt64,
    second_column UInt64,
    v String
) ENGINE = MergeTree ORDER BY (key_column, second_column);
INSERT INTO squality_dict_source_rows VALUES (1, 2, 'aaa');

CREATE DICTIONARY squality_dict_source_direct
(
    key_column UInt64,
    second_column UInt64,
    v String DEFAULT 'missing'
)
PRIMARY KEY key_column, second_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default'
    TABLE 'squality_dict_source_rows' PASSWORD '' DB currentDatabase()
    WHERE 'key_column NOT IN (99, NULL)'))
LAYOUT(COMPLEX_KEY_DIRECT())
SETTINGS(in_null_as_unknown = 1);

CREATE DICTIONARY squality_dict_source_hashed
(
    key_column UInt64,
    second_column UInt64,
    v String DEFAULT 'missing'
)
PRIMARY KEY key_column, second_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default'
    TABLE 'squality_dict_source_rows' PASSWORD '' DB currentDatabase()
    WHERE 'key_column NOT IN (99, NULL)'))
LAYOUT(COMPLEX_KEY_HASHED())
SETTINGS(in_null_as_unknown = 1);

SELECT dictGet('squality_dict_source_direct', 'v', (toUInt64(1), toUInt64(2)));
SELECT dictHas('squality_dict_source_direct', (toUInt64(1), toUInt64(2)));
SELECT dictGet('squality_dict_source_hashed', 'v', (toUInt64(1), toUInt64(2)));
SELECT dictHas('squality_dict_source_hashed', (toUInt64(1), toUInt64(2)));

DROP DICTIONARY squality_dict_source_direct;
DROP DICTIONARY squality_dict_source_hashed;
DROP TABLE squality_dict_source_rows;
