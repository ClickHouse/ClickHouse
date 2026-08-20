-- Test memory consumed by part metadata (1000 parts x 10 columns, 1 row each)
-- This measures the overhead of part structures, not data

CREATE TABLE test_many_parts (
    id UInt64,
    col1 String,
    col2 String,
    col3 String,
    col4 String,
    col5 String,
    col6 Float64,
    col7 Float64,
    col8 DateTime,
    col9 DateTime
) ENGINE = MergeTree()
PARTITION BY id  -- Each id becomes a separate partition, forcing separate parts
ORDER BY id
SETTINGS
    max_bytes_to_merge_at_max_space_in_pool = 1,
    max_bytes_to_merge_at_min_space_in_pool = 1;

-- Insert 1000 rows - each goes to its own partition = 1000 parts
INSERT INTO test_many_parts
SELECT
    number,
    'value1', 'value2', 'value3', 'value4', 'value5',
    rand(), rand(),
    now(), now()
FROM numbers(1000)
SETTINGS max_partitions_per_insert_block = 0;
