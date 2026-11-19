-- Test memory for data parts and indexes
-- A fixed time keeps every row in one `toYYYYMM(created)` partition, whatever the date of the run.
INSERT INTO test_mt SELECT
    number,
    concat('name_', toString(number)),
    rand() / 1000000000.0,
    toDateTime('2026-01-15 12:00:00') - INTERVAL number SECOND
FROM numbers(100000);

-- Force merge to single part
OPTIMIZE TABLE test_mt FINAL;
