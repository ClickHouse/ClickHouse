SET flatten_nested = 1;
SET materialize_ttl_after_modify = 0;

DROP TABLE IF EXISTS ttl_missing_nested_wide;
CREATE TABLE ttl_missing_nested_wide
(
    id UInt64,
    ts DateTime,
    n Nested(a UInt64)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO ttl_missing_nested_wide VALUES (0, '2000-01-01', [1, 2]), (1, '2000-01-01', []);
INSERT INTO ttl_missing_nested_wide VALUES (2, '2000-01-01', [3]), (3, '2000-01-01', [4, 5, 6]);
ALTER TABLE ttl_missing_nested_wide ADD COLUMN n.d Array(UInt8);

SELECT 'wide before', count(), countIf(empty(n.d)) FROM ttl_missing_nested_wide;
ALTER TABLE ttl_missing_nested_wide MODIFY TTL ts + INTERVAL 1 DAY DELETE WHERE empty(n.d);
OPTIMIZE TABLE ttl_missing_nested_wide FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'wide after', count(), countIf(empty(n.d)) FROM ttl_missing_nested_wide;
SELECT id, n.a, n.d FROM ttl_missing_nested_wide ORDER BY id;
DROP TABLE ttl_missing_nested_wide;

DROP TABLE IF EXISTS ttl_missing_nested_compact;
CREATE TABLE ttl_missing_nested_compact
(
    id UInt64,
    ts DateTime,
    n Nested(a UInt64)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000,
    max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO ttl_missing_nested_compact VALUES (0, '2000-01-01', [1, 2]), (1, '2000-01-01', []);
INSERT INTO ttl_missing_nested_compact VALUES (2, '2000-01-01', [3]), (3, '2000-01-01', [4, 5, 6]);
ALTER TABLE ttl_missing_nested_compact ADD COLUMN n.d Array(UInt8);

SELECT 'compact before', count(), countIf(empty(n.d)) FROM ttl_missing_nested_compact;
ALTER TABLE ttl_missing_nested_compact MODIFY TTL ts + INTERVAL 1 DAY DELETE WHERE empty(n.d);
OPTIMIZE TABLE ttl_missing_nested_compact FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'compact after', count(), countIf(empty(n.d)) FROM ttl_missing_nested_compact;
SELECT id, n.a, n.d FROM ttl_missing_nested_compact ORDER BY id;
DROP TABLE ttl_missing_nested_compact;

DROP TABLE IF EXISTS ttl_missing_nested_unshared;
CREATE TABLE ttl_missing_nested_unshared
(
    id UInt64,
    ts DateTime,
    n Nested(a UInt64)
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 0, min_bytes_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO ttl_missing_nested_unshared VALUES (0, '2000-01-01', [1, 2]), (1, '2000-01-01', []);
INSERT INTO ttl_missing_nested_unshared VALUES (2, '2000-01-01', [3]), (3, '2000-01-01', [4, 5, 6]);
ALTER TABLE ttl_missing_nested_unshared ADD COLUMN n.d Array(UInt8);

SELECT 'unshared before', count(), countIf(empty(n.d)) FROM ttl_missing_nested_unshared;
ALTER TABLE ttl_missing_nested_unshared MODIFY TTL ts + INTERVAL 1 DAY DELETE WHERE empty(n.d);
OPTIMIZE TABLE ttl_missing_nested_unshared FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'unshared after', count(), countIf(empty(n.d)) FROM ttl_missing_nested_unshared;
DROP TABLE ttl_missing_nested_unshared;