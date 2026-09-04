-- Test memory for MergeTree metadata structures
CREATE TABLE test_mt (
    id UInt64,
    name String,
    value Float64,
    created DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created)
ORDER BY (created, id);
