DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    key_column UInt64,
    data_column_1 UInt64,
    data_column_2 UInt8
)
ENGINE = MergeTree
ORDER BY key_column;

INSERT INTO test_table VALUES (0, 0, 0);

DROP DICTIONARY IF EXISTS test_dictionary;
CREATE DICTIONARY test_dictionary
(
    key_column UInt64 DEFAULT 0,
    data_column_1 UInt64 DEFAULT 1,
    data_column_2 UInt8 DEFAULT 1
)
PRIMARY KEY key_column
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(TABLE 'test_table'));

DROP TABLE IF EXISTS test_table_default;
CREATE TABLE test_table_default
(
    data_1 DEFAULT dictGetUInt64('test_dictionary', 'data_column_1', toUInt64(0)),
    data_2 DEFAULT dictGet(test_dictionary, 'data_column_2', toUInt64(0))
)
ENGINE=TinyLog;

INSERT INTO test_table_default(data_1) VALUES (5);
SELECT * FROM test_table_default;

DROP DICTIONARY test_dictionary;
DROP TABLE test_table;
DROP TABLE test_table_default;
