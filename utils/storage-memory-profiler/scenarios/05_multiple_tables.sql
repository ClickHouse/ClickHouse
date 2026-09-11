-- Test memory scaling with multiple tables
CREATE TABLE test_mt_2 (
    id UInt64,
    name String,
    value Float64,
    created DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created)
ORDER BY (created, id);

CREATE TABLE test_mt_3 (
    id UInt64,
    name String,
    value Float64,
    created DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created)
ORDER BY (created, id);

INSERT INTO test_mt_2 SELECT * FROM test_mt;
INSERT INTO test_mt_3 SELECT * FROM test_mt;
