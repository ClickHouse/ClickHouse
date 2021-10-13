DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table
(
    key UInt64,
    value String,
    INDEX value_index value TYPE minmax GRANULARITY 1
)
Engine=MergeTree()
ORDER BY key;

INSERT INTO test_table VALUES (0, 'Value');
SELECT * FROM system.data_skipping_indices WHERE database = currentDatabase();

DROP TABLE test_table;
