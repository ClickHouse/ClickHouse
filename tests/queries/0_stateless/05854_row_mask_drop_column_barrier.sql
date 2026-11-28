-- Tags: no-replicated-database
-- Keep the mixed commands in one `ALTER` to check their execution order.
-- A `Replicated` database rejects this mixed command sequence before mutation dispatch.

DROP TABLE IF EXISTS test_05854_barrier;

CREATE TABLE test_05854_barrier
(
    id UInt8,
    nullable_n Nullable(Int32),
    x Int64,
    spare UInt8
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
    enable_row_mask_update_coalescing = 1, enable_row_mask_action_batching = 1;

INSERT INTO test_05854_barrier VALUES
    (1, NULL, 10, 0), (2, -1, 120, 0), (3, 0, 30, 0),
    (4, 1, 40, 0), (5, 2, 50, 0), (6, NULL, 60, 0);

-- A prior mutation has already cleared some rows' `_row_exists` values.
ALTER TABLE test_05854_barrier
    UPDATE _row_exists = 0 WHERE id IN (1, 2, 3)
SETTINGS mutations_sync = 2;

-- The later mask must see the metadata change without losing earlier masks.
ALTER TABLE test_05854_barrier
    UPDATE _row_exists = 0 WHERE id = 4,
    DROP COLUMN spare,
    UPDATE _row_exists = 0 WHERE id = 5
SETTINGS mutations_sync = 2;

SELECT 'barrier_mask', id, _row_exists
FROM test_05854_barrier ORDER BY id SETTINGS apply_deleted_mask = 0;
SELECT 'barrier', id, nullable_n, x FROM test_05854_barrier ORDER BY id;

DROP TABLE test_05854_barrier;
