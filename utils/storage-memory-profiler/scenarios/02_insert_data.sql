-- Test memory for data parts and indexes
INSERT INTO test_mt SELECT
    number,
    concat('name_', toString(number)),
    rand() / 1000000000.0,
    now() - INTERVAL number SECOND
FROM numbers(100000);

-- Force merge to single part
OPTIMIZE TABLE test_mt FINAL;
