-- Tags: shared-catalog

DROP TABLE IF EXISTS test_05857_off;
DROP TABLE IF EXISTS test_05857_on;

CREATE TABLE test_05857_off
(
    k UInt64,
    s AggregateFunction(sum, Int64)
)
ENGINE = SharedAggregatingMergeTree() ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
    number_of_free_entries_in_pool_to_execute_mutation = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0,
    enable_row_mask_update_coalescing = 0, enable_row_mask_action_batching = 0,
    enable_row_mask_typed_set_union = 0;

CREATE TABLE test_05857_on
(
    k UInt64,
    s AggregateFunction(sum, Int64)
)
ENGINE = SharedAggregatingMergeTree() ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
    number_of_free_entries_in_pool_to_execute_mutation = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0,
    enable_row_mask_update_coalescing = 0, enable_row_mask_action_batching = 0,
    enable_row_mask_typed_set_union = 1;

INSERT INTO test_05857_off
SELECT k, sumState(v)
FROM values('k UInt64, v Int64', (1, 10), (2, 20), (3, 30), (4, 40))
GROUP BY k;
INSERT INTO test_05857_on SELECT * FROM test_05857_off;

-- Overlapping ready subquery `Set`s must agree with and without `enable_row_mask_typed_set_union`.
ALTER TABLE test_05857_off
    UPDATE _row_exists = 0 WHERE k IN (SELECT toUInt64(number * 2 + 1) FROM numbers(2)),
    UPDATE _row_exists = 0 WHERE k IN (SELECT toUInt64(number + 3) FROM numbers(2))
SETTINGS mutations_sync = 2;
ALTER TABLE test_05857_on
    UPDATE _row_exists = 0 WHERE k IN (SELECT toUInt64(number * 2 + 1) FROM numbers(2)),
    UPDATE _row_exists = 0 WHERE k IN (SELECT toUInt64(number + 3) FROM numbers(2))
SETTINGS mutations_sync = 2;

SELECT 'before_off', k, sumMerge(s) FROM test_05857_off GROUP BY k ORDER BY k;
SELECT 'before_on', k, sumMerge(s) FROM test_05857_on GROUP BY k ORDER BY k;

-- Fresh states at deleted keys must remain visible after a merge.
INSERT INTO test_05857_off
SELECT k, sumState(v)
FROM values('k UInt64, v Int64', (1, 7), (3, 5))
GROUP BY k;
INSERT INTO test_05857_on
SELECT k, sumState(v)
FROM values('k UInt64, v Int64', (1, 7), (3, 5))
GROUP BY k;
OPTIMIZE TABLE test_05857_off FINAL SETTINGS alter_sync = 1;
OPTIMIZE TABLE test_05857_on FINAL SETTINGS alter_sync = 1;

SELECT 'after_off', k, sumMerge(s) FROM test_05857_off GROUP BY k ORDER BY k;
SELECT 'after_on', k, sumMerge(s) FROM test_05857_on GROUP BY k ORDER BY k;

DROP TABLE test_05857_off;
DROP TABLE test_05857_on;
