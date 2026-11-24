DROP DICTIONARY IF EXISTS squality_dict_compound;
DROP TABLE IF EXISTS squality_dict_compound_source;
CREATE TABLE squality_dict_compound_source
(
    key_column UInt64,
    second_column UInt64,
    v String
) ENGINE = MergeTree ORDER BY (key_column, second_column);
INSERT INTO squality_dict_compound_source VALUES (1, 2, 'aaa');

CREATE DICTIONARY squality_dict_compound
(
    key_column UInt64,
    second_column UInt64,
    v String DEFAULT 'missing'
)
PRIMARY KEY key_column, second_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default'
    TABLE 'squality_dict_compound_source' PASSWORD '' DB currentDatabase()))
LAYOUT(COMPLEX_KEY_DIRECT())
SETTINGS(in_null_as_unknown = 1);

SELECT dictGet('squality_dict_compound', 'v', (toUInt64(1), toUInt64(2)));
SELECT dictHas('squality_dict_compound', (toUInt64(1), toUInt64(2)));

DROP DICTIONARY squality_dict_compound;
DROP TABLE squality_dict_compound_source;
