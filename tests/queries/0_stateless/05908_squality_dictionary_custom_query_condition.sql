DROP DICTIONARY IF EXISTS squality_dict_custom_condition;
DROP TABLE IF EXISTS squality_dict_custom_source;

CREATE TABLE squality_dict_custom_source
(
    k1 UInt64,
    k2 UInt64,
    v String,
    allowed UInt8
) ENGINE = Memory;

INSERT INTO squality_dict_custom_source VALUES
    (1, 2, 'public', 1),
    (3, 4, 'secret', 0);

CREATE DICTIONARY squality_dict_custom_condition
(
    k1 UInt64,
    k2 UInt64,
    v String DEFAULT 'missing'
)
PRIMARY KEY k1, k2
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' PASSWORD '' DB currentDatabase()
    QUERY $$SELECT k1, k2, v FROM squality_dict_custom_source WHERE allowed = 1 AND {condition}$$))
LAYOUT(COMPLEX_KEY_DIRECT())
SETTINGS(in_null_as_unknown = 1);

SELECT dictGet('squality_dict_custom_condition', 'v', (k1, k2))
FROM values('k1 UInt64, k2 UInt64', (1, 2), (3, 4));

DROP DICTIONARY squality_dict_custom_condition;
DROP TABLE squality_dict_custom_source;
