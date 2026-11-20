DROP TABLE IF EXISTS test_05814_off;
DROP TABLE IF EXISTS test_05814_on;
DROP TABLE IF EXISTS test_05814_fallback;

CREATE TABLE test_05814_off
(
    tenant Int32,
    key_gap UInt16,
    item_key UInt128,
    s AggregateFunction(sum, Int64)
)
ENGINE = AggregatingMergeTree ORDER BY (tenant, key_gap, item_key)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
    enable_row_mask_update_coalescing = 1, enable_row_mask_action_batching = 0;

CREATE TABLE test_05814_on
(
    tenant Int32,
    key_gap UInt16,
    item_key UInt128,
    s AggregateFunction(sum, Int64)
)
ENGINE = AggregatingMergeTree ORDER BY (tenant, key_gap, item_key)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
    enable_row_mask_update_coalescing = 1, enable_row_mask_action_batching = 1;

-- Repeated input groups and repeated states in separate parts.
INSERT INTO test_05814_off
SELECT seed_tenant, toUInt16(seed_key % 3), toUInt128(seed_key), sumState(seed_value)
FROM values('seed_tenant Int32, seed_key UInt64, seed_value Int64',
    (-1, 1, 10), (-1, 2, 20), (-1, 7, 30),
    (0, 1, 40), (0, 4, 50), (0, 9, 60),
    (1, 2, 70), (1, 7, 80), (1, 12, 90)) AS seed_rows
CROSS JOIN numbers(2) AS seed_repetitions
GROUP BY seed_tenant, seed_key;
INSERT INTO test_05814_on SELECT * FROM test_05814_off;
INSERT INTO test_05814_off SELECT * FROM test_05814_off;
INSERT INTO test_05814_on SELECT * FROM test_05814_on;
OPTIMIZE TABLE test_05814_off FINAL;
OPTIMIZE TABLE test_05814_on FINAL;

-- A later batch must preserve an already cleared `_row_exists` value.
ALTER TABLE test_05814_off
    UPDATE _row_exists = 0 WHERE tenant = 1 AND item_key = toUInt128(12)
SETTINGS mutations_sync = 2;
ALTER TABLE test_05814_on
    UPDATE _row_exists = 0 WHERE tenant = 1 AND item_key = toUInt128(12)
SETTINGS mutations_sync = 2;

-- Signed comparisons, ranges, `OR`, overlap and a missing key bypass the legacy singleton shape.
ALTER TABLE test_05814_off
    UPDATE _row_exists = 0 WHERE tenant = -1 AND item_key < toUInt128(3),
    UPDATE _row_exists = 0 WHERE tenant = 0 AND item_key >= toUInt128(4),
    UPDATE _row_exists = 0 WHERE (tenant = 1 AND item_key = toUInt128(7)) OR item_key = toUInt128(999),
    UPDATE _row_exists = 0 WHERE tenant = -1 AND item_key = toUInt128(1)
SETTINGS mutations_sync = 2;
ALTER TABLE test_05814_on
    UPDATE _row_exists = 0 WHERE tenant = -1 AND item_key < toUInt128(3),
    UPDATE _row_exists = 0 WHERE tenant = 0 AND item_key >= toUInt128(4),
    UPDATE _row_exists = 0 WHERE (tenant = 1 AND item_key = toUInt128(7)) OR item_key = toUInt128(999),
    UPDATE _row_exists = 0 WHERE tenant = -1 AND item_key = toUInt128(1)
SETTINGS mutations_sync = 2;

SELECT 'mask_off', tenant, item_key, _row_exists
FROM test_05814_off ORDER BY tenant, item_key SETTINGS apply_deleted_mask = 0;
SELECT 'mask_on', tenant, item_key, _row_exists
FROM test_05814_on ORDER BY tenant, item_key SETTINGS apply_deleted_mask = 0;
SELECT 'aggregate_off', tenant, item_key, sumMerge(s)
FROM test_05814_off GROUP BY tenant, item_key ORDER BY tenant, item_key;
SELECT 'aggregate_on', tenant, item_key, sumMerge(s)
FROM test_05814_on GROUP BY tenant, item_key ORDER BY tenant, item_key;

-- New states at deleted keys must survive; surviving states still merge normally.
INSERT INTO test_05814_off
SELECT post_off_tenant, toUInt16(post_off_key % 3), toUInt128(post_off_key), sumState(post_off_value)
FROM values('post_off_tenant Int32, post_off_key UInt64, post_off_value Int64',
    (-1, 1, 7), (1, 2, 5), (1, 12, 11))
GROUP BY post_off_tenant, post_off_key;
INSERT INTO test_05814_on
SELECT post_on_tenant, toUInt16(post_on_key % 3), toUInt128(post_on_key), sumState(post_on_value)
FROM values('post_on_tenant Int32, post_on_key UInt64, post_on_value Int64',
    (-1, 1, 7), (1, 2, 5), (1, 12, 11))
GROUP BY post_on_tenant, post_on_key;
OPTIMIZE TABLE test_05814_off FINAL;
OPTIMIZE TABLE test_05814_on FINAL;
SELECT 'merged_off', tenant, item_key, sumMerge(s)
FROM test_05814_off GROUP BY tenant, item_key ORDER BY tenant, item_key;
SELECT 'merged_on', tenant, item_key, sumMerge(s)
FROM test_05814_on GROUP BY tenant, item_key ORDER BY tenant, item_key;
SELECT 'merged_mask_off', count(), countIf(_row_exists = 0)
FROM test_05814_off SETTINGS apply_deleted_mask = 0;
SELECT 'merged_mask_on', count(), countIf(_row_exists = 0)
FROM test_05814_on SETTINGS apply_deleted_mask = 0;

CREATE TABLE test_05814_fallback
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
INSERT INTO test_05814_fallback VALUES
    (1, NULL, 10, 0), (2, -1, 20, 0), (3, 0, 30, 0),
    (4, 1, 40, 0), (5, 2, 50, 0), (6, NULL, 60, 0);

-- The last mask must see the value written by the ordinary `UPDATE`.
ALTER TABLE test_05814_fallback
    UPDATE _row_exists = 0 WHERE x < 20,
    UPDATE x = x + 100 WHERE id = 2,
    UPDATE _row_exists = 0 WHERE x > 100
SETTINGS mutations_sync = 2;
SELECT 'mixed', id, x, _row_exists
FROM test_05814_fallback ORDER BY id SETTINGS apply_deleted_mask = 0;

-- A literal `Set` and nullable predicates retain their ordinary semantics.
ALTER TABLE test_05814_fallback
    UPDATE _row_exists = 0 WHERE id IN (3, 99),
    UPDATE _row_exists = 0 WHERE nullable_n < 1
SETTINGS mutations_sync = 2;
SELECT 'set_nullable', id, nullable_n, x FROM test_05814_fallback ORDER BY id;

-- `DROP COLUMN` remains a barrier even when it needs no interpreter stage.
ALTER TABLE test_05814_fallback
    UPDATE _row_exists = 0 WHERE id = 4,
    DROP COLUMN spare,
    UPDATE _row_exists = 0 WHERE id = 5
SETTINGS mutations_sync = 2;
SELECT 'barrier_mask', id, _row_exists
FROM test_05814_fallback ORDER BY id SETTINGS apply_deleted_mask = 0;
SELECT 'barrier', id, nullable_n, x FROM test_05814_fallback ORDER BY id;

DROP TABLE test_05814_off;
DROP TABLE test_05814_on;
DROP TABLE test_05814_fallback;
