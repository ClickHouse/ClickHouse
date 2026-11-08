SET flatten_nested = 1;
SET materialize_ttl_after_modify = 0;

-- The tables stay on horizontal merges, which these TTL rules take. A background TTL merge can drop `n.a`
-- before `OPTIMIZE`, which then has no TTL left to apply and with randomized settings could merge vertically.

DROP TABLE IF EXISTS ttl_nested_full_wide;
CREATE TABLE ttl_nested_full_wide (id UInt64, ts DateTime, n Nested(a UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
ALTER TABLE ttl_nested_full_wide MODIFY COLUMN n.a Array(UInt64) TTL ts + INTERVAL 1 DAY;
INSERT INTO ttl_nested_full_wide VALUES (0, '2000-01-01', [1, 2]), (1, '2000-01-01', [3]);
ALTER TABLE ttl_nested_full_wide ADD COLUMN n.d Array(UInt8);
ALTER TABLE ttl_nested_full_wide ADD COLUMN m UInt64 MATERIALIZED length(n.d);
ALTER TABLE ttl_nested_full_wide ADD PROJECTION p (SELECT id, length(n.d) AS len ORDER BY id);

OPTIMIZE TABLE ttl_nested_full_wide FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'full wide base', sum(length(n.d)) FROM ttl_nested_full_wide SETTINGS optimize_use_projections = 0;
SELECT 'full wide projection', sum(length(n.d)) FROM ttl_nested_full_wide
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT id, m, length(n.d) FROM ttl_nested_full_wide ORDER BY id SETTINGS optimize_use_projections = 0;
SELECT 'full wide stored', count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_full_wide' AND active AND column = 'n.d';
SELECT 'full wide source', count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_full_wide' AND active AND column = 'n.a';
DROP TABLE ttl_nested_full_wide;

DROP TABLE IF EXISTS ttl_nested_full_compact;
CREATE TABLE ttl_nested_full_compact (id UInt64, ts DateTime, n Nested(a UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
ALTER TABLE ttl_nested_full_compact MODIFY COLUMN n.a Array(UInt64) TTL ts + INTERVAL 1 DAY;
INSERT INTO ttl_nested_full_compact VALUES (0, '2000-01-01', [1, 2]), (1, '2000-01-01', [3]);
ALTER TABLE ttl_nested_full_compact ADD COLUMN n.d Array(UInt8);
ALTER TABLE ttl_nested_full_compact ADD COLUMN m UInt64 MATERIALIZED length(n.d);
ALTER TABLE ttl_nested_full_compact ADD PROJECTION p (SELECT id, length(n.d) AS len ORDER BY id);

OPTIMIZE TABLE ttl_nested_full_compact FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'full compact base', sum(length(n.d)) FROM ttl_nested_full_compact SETTINGS optimize_use_projections = 0;
SELECT 'full compact projection', sum(length(n.d)) FROM ttl_nested_full_compact
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT id, m, length(n.d) FROM ttl_nested_full_compact ORDER BY id SETTINGS optimize_use_projections = 0;
SELECT 'full compact stored', count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_full_compact' AND active AND column = 'n.d';
DROP TABLE ttl_nested_full_compact;

DROP TABLE IF EXISTS ttl_nested_partial_wide;
CREATE TABLE ttl_nested_partial_wide (id UInt64, ts DateTime, n Nested(a UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO ttl_nested_partial_wide VALUES (0, '2000-01-01', [1, 2]), (1, '2099-01-01', [3]);
ALTER TABLE ttl_nested_partial_wide ADD COLUMN n.d Array(UInt8);
ALTER TABLE ttl_nested_partial_wide MODIFY COLUMN n.a Array(UInt64) TTL ts + INTERVAL 1 DAY;
ALTER TABLE ttl_nested_partial_wide ADD COLUMN m UInt64 MATERIALIZED length(n.d);
ALTER TABLE ttl_nested_partial_wide ADD PROJECTION p (SELECT id, length(n.d) AS len ORDER BY id);

OPTIMIZE TABLE ttl_nested_partial_wide FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'partial wide base', sum(length(n.d)) FROM ttl_nested_partial_wide SETTINGS optimize_use_projections = 0;
SELECT 'partial wide projection', sum(length(n.d)) FROM ttl_nested_partial_wide
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT id, m, length(n.d) FROM ttl_nested_partial_wide ORDER BY id SETTINGS optimize_use_projections = 0;
SELECT 'partial wide stored', count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_partial_wide' AND active AND column = 'n.d';
SELECT 'partial wide source', count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_partial_wide' AND active AND column = 'n.a';
DROP TABLE ttl_nested_partial_wide;

DROP TABLE IF EXISTS ttl_nested_partial_compact;
CREATE TABLE ttl_nested_partial_compact (id UInt64, ts DateTime, n Nested(a UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO ttl_nested_partial_compact VALUES (0, '2000-01-01', [1, 2]), (1, '2099-01-01', [3]);
ALTER TABLE ttl_nested_partial_compact ADD COLUMN n.d Array(UInt8);
ALTER TABLE ttl_nested_partial_compact MODIFY COLUMN n.a Array(UInt64) TTL ts + INTERVAL 1 DAY;
ALTER TABLE ttl_nested_partial_compact ADD COLUMN m UInt64 MATERIALIZED length(n.d);
ALTER TABLE ttl_nested_partial_compact ADD PROJECTION p (SELECT id, length(n.d) AS len ORDER BY id);

OPTIMIZE TABLE ttl_nested_partial_compact FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'partial compact base', sum(length(n.d)) FROM ttl_nested_partial_compact SETTINGS optimize_use_projections = 0;
SELECT 'partial compact projection', sum(length(n.d)) FROM ttl_nested_partial_compact
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT id, m, length(n.d) FROM ttl_nested_partial_compact ORDER BY id SETTINGS optimize_use_projections = 0;
SELECT 'partial compact stored', count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_partial_compact' AND active AND column = 'n.d';
DROP TABLE ttl_nested_partial_compact;

DROP TABLE IF EXISTS ttl_nested_recomputed_source;
CREATE TABLE ttl_nested_recomputed_source
(
    id UInt64,
    ts DateTime,
    x UInt8 TTL ts + INTERVAL 1 DAY,
    `n.a` Array(UInt64) MATERIALIZED range(x)
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO ttl_nested_recomputed_source (id, ts, x) VALUES (0, '2000-01-01', 2), (1, '2000-01-01', 1);
ALTER TABLE ttl_nested_recomputed_source ADD COLUMN n.d Array(UInt8);
ALTER TABLE ttl_nested_recomputed_source ADD COLUMN m UInt64 MATERIALIZED length(n.d) AFTER id;
ALTER TABLE ttl_nested_recomputed_source ADD PROJECTION p (SELECT id, length(n.d) AS len ORDER BY id);

OPTIMIZE TABLE ttl_nested_recomputed_source FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'recomputed base', sum(length(n.d)) FROM ttl_nested_recomputed_source SETTINGS optimize_use_projections = 0;
SELECT 'recomputed projection', sum(length(n.d)) FROM ttl_nested_recomputed_source
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT id, m, length(n.d) FROM ttl_nested_recomputed_source ORDER BY id SETTINGS optimize_use_projections = 0;
SELECT 'recomputed stored', count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_recomputed_source' AND active AND column = 'n.d';
DROP TABLE ttl_nested_recomputed_source;

DROP TABLE IF EXISTS ttl_nested_rollup_wide;
CREATE TABLE ttl_nested_rollup_wide (id UInt64, ts DateTime, n Nested(a UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
-- The first group flushes from `consume`; the last group flushes from `generate`.
INSERT INTO ttl_nested_rollup_wide VALUES (0, '2000-01-01', [1, 2]), (0, '2000-01-01', [3]), (1, '2099-01-01', [7, 8, 9, 10]), (2, '2000-01-01', [4, 5]), (2, '2000-01-01', [6]);
ALTER TABLE ttl_nested_rollup_wide ADD COLUMN n.d Array(UInt8);
ALTER TABLE ttl_nested_rollup_wide ADD COLUMN m UInt64 MATERIALIZED length(n.d);
ALTER TABLE ttl_nested_rollup_wide ADD PROJECTION p (SELECT id, length(n.d) AS len ORDER BY id);
ALTER TABLE ttl_nested_rollup_wide MODIFY TTL ts + INTERVAL 1 DAY
    GROUP BY id SET `n.a` = arraySlice(any(n.a), 1, 0);

OPTIMIZE TABLE ttl_nested_rollup_wide FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'rollup wide base', sum(length(n.d)) FROM ttl_nested_rollup_wide SETTINGS optimize_use_projections = 0;
SELECT 'rollup wide projection', sum(length(n.d)) FROM ttl_nested_rollup_wide
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT id, m, length(n.d) FROM ttl_nested_rollup_wide ORDER BY id SETTINGS optimize_use_projections = 0;
SELECT 'rollup wide stored', count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_rollup_wide' AND active AND column = 'n.d';
DROP TABLE ttl_nested_rollup_wide;

DROP TABLE IF EXISTS ttl_nested_rollup_compact;
CREATE TABLE ttl_nested_rollup_compact (id UInt64, ts DateTime, n Nested(a UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO ttl_nested_rollup_compact VALUES (0, '2000-01-01', [1, 2]), (0, '2000-01-01', [3]), (1, '2099-01-01', [7, 8, 9, 10]), (2, '2000-01-01', [4, 5]), (2, '2000-01-01', [6]);
ALTER TABLE ttl_nested_rollup_compact ADD COLUMN n.d Array(UInt8);
ALTER TABLE ttl_nested_rollup_compact ADD COLUMN m UInt64 MATERIALIZED length(n.d);
ALTER TABLE ttl_nested_rollup_compact ADD PROJECTION p (SELECT id, length(n.d) AS len ORDER BY id);
ALTER TABLE ttl_nested_rollup_compact MODIFY TTL ts + INTERVAL 1 DAY
    GROUP BY id SET `n.a` = arraySlice(any(n.a), 1, 0);

OPTIMIZE TABLE ttl_nested_rollup_compact FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'rollup compact base', sum(length(n.d)) FROM ttl_nested_rollup_compact SETTINGS optimize_use_projections = 0;
SELECT 'rollup compact projection', sum(length(n.d)) FROM ttl_nested_rollup_compact
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT id, m, length(n.d) FROM ttl_nested_rollup_compact ORDER BY id SETTINGS optimize_use_projections = 0;
SELECT 'rollup compact stored', count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_rollup_compact' AND active AND column = 'n.d';
DROP TABLE ttl_nested_rollup_compact;

-- `n.d` is read only by the `SET` assignment, without a projection or a materialized column.
DROP TABLE IF EXISTS ttl_nested_rollup_set_wide;
CREATE TABLE ttl_nested_rollup_set_wide (id UInt64, ts DateTime, n Nested(a UInt64), total UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO ttl_nested_rollup_set_wide VALUES (0, '2000-01-01', [1, 2], 0), (0, '2000-01-01', [3, 4], 0), (1, '2099-01-01', [5, 6, 7], 0), (2, '2000-01-01', [8], 0), (2, '2000-01-01', [9], 0);
ALTER TABLE ttl_nested_rollup_set_wide ADD COLUMN n.d Array(UInt8);
ALTER TABLE ttl_nested_rollup_set_wide MODIFY TTL ts + INTERVAL 1 DAY GROUP BY id SET total = length(any(n.d));

OPTIMIZE TABLE ttl_nested_rollup_set_wide FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'rollup set wide', id, total, length(n.d) FROM ttl_nested_rollup_set_wide ORDER BY id;
SELECT 'rollup set wide stored', count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_rollup_set_wide' AND active AND column = 'n.d';
DROP TABLE ttl_nested_rollup_set_wide;

DROP TABLE IF EXISTS ttl_nested_rollup_set_compact;
CREATE TABLE ttl_nested_rollup_set_compact (id UInt64, ts DateTime, n Nested(a UInt64), total UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO ttl_nested_rollup_set_compact VALUES (0, '2000-01-01', [1, 2], 0), (0, '2000-01-01', [3, 4], 0), (1, '2099-01-01', [5, 6, 7], 0), (2, '2000-01-01', [8], 0), (2, '2000-01-01', [9], 0);
ALTER TABLE ttl_nested_rollup_set_compact ADD COLUMN n.d Array(UInt8);
ALTER TABLE ttl_nested_rollup_set_compact MODIFY TTL ts + INTERVAL 1 DAY GROUP BY id SET total = length(any(n.d));

OPTIMIZE TABLE ttl_nested_rollup_set_compact FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'rollup set compact', id, total, length(n.d) FROM ttl_nested_rollup_set_compact ORDER BY id;
SELECT 'rollup set compact stored', count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_rollup_set_compact' AND active AND column = 'n.d';
DROP TABLE ttl_nested_rollup_set_compact;

-- `n.d` is read only through an `ALIAS` or an `EPHEMERAL` column, and the TTL does not read it.
DROP TABLE IF EXISTS ttl_nested_through_alias;
CREATE TABLE ttl_nested_through_alias (id UInt64, ts DateTime, n Nested(a UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO ttl_nested_through_alias VALUES (0, '2000-01-01', [1, 2]), (1, '2000-01-01', []), (2, '2000-01-01', [3, 4, 5]);
ALTER TABLE ttl_nested_through_alias ADD COLUMN n.d Array(UInt8);
ALTER TABLE ttl_nested_through_alias ADD COLUMN al UInt64 ALIAS length(n.d);
ALTER TABLE ttl_nested_through_alias ADD COLUMN m UInt64 MATERIALIZED al * 10;
ALTER TABLE ttl_nested_through_alias MODIFY TTL ts + INTERVAL 1 DAY DELETE WHERE id = 1;

OPTIMIZE TABLE ttl_nested_through_alias FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'through alias', id, m, length(n.d) FROM ttl_nested_through_alias ORDER BY id;
SELECT 'through alias stored', arraySort(groupUniqArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_through_alias' AND active
    AND column IN ('id', 'ts', 'n.a', 'n.d', 'm');
DROP TABLE ttl_nested_through_alias;

DROP TABLE IF EXISTS ttl_nested_through_ephemeral;
CREATE TABLE ttl_nested_through_ephemeral (id UInt64, ts DateTime, n Nested(a UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO ttl_nested_through_ephemeral VALUES (0, '2000-01-01', [1, 2]), (1, '2000-01-01', []), (2, '2000-01-01', [3, 4, 5]);
ALTER TABLE ttl_nested_through_ephemeral ADD COLUMN n.d Array(UInt8);
ALTER TABLE ttl_nested_through_ephemeral ADD COLUMN e UInt64 EPHEMERAL length(n.d);
ALTER TABLE ttl_nested_through_ephemeral ADD COLUMN x UInt64 DEFAULT e * 10;
ALTER TABLE ttl_nested_through_ephemeral MODIFY TTL ts + INTERVAL 1 DAY DELETE WHERE id = 1;

OPTIMIZE TABLE ttl_nested_through_ephemeral FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'through ephemeral', id, x, length(n.d) FROM ttl_nested_through_ephemeral ORDER BY id;
SELECT 'through ephemeral stored', arraySort(groupUniqArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_through_ephemeral' AND active
    AND column IN ('id', 'ts', 'n.a', 'n.d', 'x');
DROP TABLE ttl_nested_through_ephemeral;

DROP TABLE IF EXISTS ttl_nested_default_source_wide;
CREATE TABLE ttl_nested_default_source_wide (id UInt64, ts DateTime, n Nested(a UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO ttl_nested_default_source_wide VALUES (0, '2000-01-01', [7, 8]);
INSERT INTO ttl_nested_default_source_wide VALUES (1, '2000-01-01', [9]);
ALTER TABLE ttl_nested_default_source_wide ADD COLUMN n.d Array(UInt8);
ALTER TABLE ttl_nested_default_source_wide ADD COLUMN m UInt64 MATERIALIZED length(n.d);
ALTER TABLE ttl_nested_default_source_wide ADD PROJECTION p (SELECT id, length(n.d) AS len ORDER BY id);
ALTER TABLE ttl_nested_default_source_wide MODIFY COLUMN n.a Array(UInt64) DEFAULT [1, 2] TTL ts + INTERVAL 1 DAY;

OPTIMIZE TABLE ttl_nested_default_source_wide FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'default source wide base', sum(length(n.d)) FROM ttl_nested_default_source_wide SETTINGS optimize_use_projections = 0;
SELECT 'default source wide projection', sum(length(n.d)) FROM ttl_nested_default_source_wide
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT id, m, length(n.d) FROM ttl_nested_default_source_wide ORDER BY id SETTINGS optimize_use_projections = 0;
SELECT 'default source wide stored', arraySort(groupUniqArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_default_source_wide' AND active
    AND column IN ('id', 'ts', 'n.a', 'n.d', 'm');
DROP TABLE ttl_nested_default_source_wide;

DROP TABLE IF EXISTS ttl_nested_default_source_compact;
CREATE TABLE ttl_nested_default_source_compact (id UInt64, ts DateTime, n Nested(a UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO ttl_nested_default_source_compact VALUES (0, '2000-01-01', [7, 8]);
INSERT INTO ttl_nested_default_source_compact VALUES (1, '2000-01-01', [9]);
ALTER TABLE ttl_nested_default_source_compact ADD COLUMN n.d Array(UInt8);
ALTER TABLE ttl_nested_default_source_compact ADD COLUMN m UInt64 MATERIALIZED length(n.d);
ALTER TABLE ttl_nested_default_source_compact ADD PROJECTION p (SELECT id, length(n.d) AS len ORDER BY id);
ALTER TABLE ttl_nested_default_source_compact MODIFY COLUMN n.a Array(UInt64) DEFAULT [1, 2] TTL ts + INTERVAL 1 DAY;

OPTIMIZE TABLE ttl_nested_default_source_compact FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'default source compact base', sum(length(n.d)) FROM ttl_nested_default_source_compact SETTINGS optimize_use_projections = 0;
SELECT 'default source compact projection', sum(length(n.d)) FROM ttl_nested_default_source_compact
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT id, m, length(n.d) FROM ttl_nested_default_source_compact ORDER BY id SETTINGS optimize_use_projections = 0;
SELECT 'default source compact stored', arraySort(groupUniqArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_default_source_compact' AND active
    AND column IN ('id', 'ts', 'n.a', 'n.d', 'm');
DROP TABLE ttl_nested_default_source_compact;
