SET flatten_nested = 1;
SET materialize_ttl_after_modify = 0;

DROP TABLE IF EXISTS ttl_missing_nested_default;
CREATE TABLE ttl_missing_nested_default (id UInt64, ts DateTime)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO ttl_missing_nested_default VALUES (0, '2000-01-01');
INSERT INTO ttl_missing_nested_default VALUES (1, '2000-01-01');
ALTER TABLE ttl_missing_nested_default ADD COLUMN n.a Array(UInt64) DEFAULT [1, 2];
ALTER TABLE ttl_missing_nested_default ADD COLUMN n.d Array(UInt8);

SELECT 'default before', count(), countIf(notEmpty(n.d)) FROM ttl_missing_nested_default;
ALTER TABLE ttl_missing_nested_default MODIFY TTL ts + INTERVAL 1 DAY DELETE WHERE notEmpty(n.d);
OPTIMIZE TABLE ttl_missing_nested_default FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'default after', id FROM ttl_missing_nested_default ORDER BY id;
SELECT 'default stored', arraySort(groupUniqArray(column))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_missing_nested_default' AND active
    AND column IN ('id', 'ts', 'n.a', 'n.d');
DROP TABLE ttl_missing_nested_default;

DROP TABLE IF EXISTS ttl_missing_nested_mixed_wide;
CREATE TABLE ttl_missing_nested_mixed_wide (id UInt64, ts DateTime)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_ttl_delete = 1;

INSERT INTO ttl_missing_nested_mixed_wide VALUES (0, '2000-01-01');
ALTER TABLE ttl_missing_nested_mixed_wide ADD COLUMN n.a Array(UInt64) DEFAULT [1, 2];
INSERT INTO ttl_missing_nested_mixed_wide VALUES (1, '2000-01-01', []), (2, '2000-01-01', [5]);
ALTER TABLE ttl_missing_nested_mixed_wide ADD COLUMN n.d Array(UInt8);

SELECT 'mixed wide before', id, n.a, n.d FROM ttl_missing_nested_mixed_wide ORDER BY id;
ALTER TABLE ttl_missing_nested_mixed_wide MODIFY TTL ts + INTERVAL 1 DAY DELETE WHERE notEmpty(n.d);
OPTIMIZE TABLE ttl_missing_nested_mixed_wide FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'mixed wide after', id FROM ttl_missing_nested_mixed_wide ORDER BY id;
SELECT 'mixed wide stored', arraySort(groupUniqArray(column))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_missing_nested_mixed_wide' AND active
    AND column IN ('id', 'ts', 'n.a', 'n.d');
DROP TABLE ttl_missing_nested_mixed_wide;

DROP TABLE IF EXISTS ttl_missing_nested_mixed_compact;
CREATE TABLE ttl_missing_nested_mixed_compact (id UInt64, ts DateTime)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000,
    max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO ttl_missing_nested_mixed_compact VALUES (0, '2000-01-01');
ALTER TABLE ttl_missing_nested_mixed_compact ADD COLUMN n.a Array(UInt64) DEFAULT [1, 2];
INSERT INTO ttl_missing_nested_mixed_compact VALUES (1, '2000-01-01', []), (2, '2000-01-01', [5]);
ALTER TABLE ttl_missing_nested_mixed_compact ADD COLUMN n.d Array(UInt8);

SELECT 'mixed compact before', id, n.a, n.d FROM ttl_missing_nested_mixed_compact ORDER BY id;
ALTER TABLE ttl_missing_nested_mixed_compact MODIFY TTL ts + INTERVAL 1 DAY DELETE WHERE notEmpty(n.d);
OPTIMIZE TABLE ttl_missing_nested_mixed_compact FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'mixed compact after', id FROM ttl_missing_nested_mixed_compact ORDER BY id;
SELECT 'mixed compact stored', arraySort(groupUniqArray(column))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_missing_nested_mixed_compact' AND active
    AND column IN ('id', 'ts', 'n.a', 'n.d');
DROP TABLE ttl_missing_nested_mixed_compact;
