-- Test memory for MergeTree metadata structures
-- The scenarios measure the memory of the storage code for a fixed table layout,
-- so pin the implicit min-max indices instead of following the server default.
CREATE TABLE test_mt (
    id UInt64,
    name String,
    value Float64,
    created DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created)
ORDER BY (created, id)
SETTINGS add_minmax_index_for_numeric_columns = 0;
