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

-- `x` is stored only in the second part, so the reader of the first one still evaluates its default.
DROP TABLE IF EXISTS ttl_nested_partly_stored_default;
CREATE TABLE ttl_nested_partly_stored_default (id UInt64, ts DateTime, n Nested(a UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO ttl_nested_partly_stored_default VALUES (0, '2000-01-01', [1, 2]), (1, '2000-01-01', []);
ALTER TABLE ttl_nested_partly_stored_default ADD COLUMN x UInt64;
INSERT INTO ttl_nested_partly_stored_default VALUES (2, '2000-01-01', [3, 4, 5], 7), (3, '2000-01-01', [6], 7);
ALTER TABLE ttl_nested_partly_stored_default ADD COLUMN n.d Array(UInt8);
ALTER TABLE ttl_nested_partly_stored_default MODIFY COLUMN x UInt64 DEFAULT length(n.d) * 10;
ALTER TABLE ttl_nested_partly_stored_default MODIFY TTL ts + INTERVAL 1 DAY DELETE WHERE id = 1;

OPTIMIZE TABLE ttl_nested_partly_stored_default FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'partly stored default', id, x, length(n.d) FROM ttl_nested_partly_stored_default ORDER BY id;
SELECT 'partly stored default stored', arraySort(groupUniqArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_partly_stored_default' AND active
    AND column IN ('id', 'ts', 'n.a', 'n.d', 'x');
DROP TABLE ttl_nested_partly_stored_default;

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

-- Nothing reads `n.d` during the merge, or only a skip index does. The base table must still keep `n.a`,
-- whose offsets `n.d` follows, exactly as it does with the projection above.
DROP TABLE IF EXISTS ttl_nested_default_source_unread_wide;
CREATE TABLE ttl_nested_default_source_unread_wide (id UInt64, ts DateTime, n Nested(a UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO ttl_nested_default_source_unread_wide VALUES (0, '2000-01-01', [7, 8]);
INSERT INTO ttl_nested_default_source_unread_wide VALUES (1, '2000-01-01', [9]);
ALTER TABLE ttl_nested_default_source_unread_wide ADD COLUMN n.d Array(UInt8);
ALTER TABLE ttl_nested_default_source_unread_wide MODIFY COLUMN n.a Array(UInt64) DEFAULT [1, 2] TTL ts + INTERVAL 1 DAY;

OPTIMIZE TABLE ttl_nested_default_source_unread_wide FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'default source unread wide', id, n.a, length(n.d) FROM ttl_nested_default_source_unread_wide ORDER BY id;
SELECT 'default source unread wide stored', arraySort(groupUniqArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_default_source_unread_wide' AND active
    AND column IN ('id', 'ts', 'n.a', 'n.d');
DROP TABLE ttl_nested_default_source_unread_wide;

DROP TABLE IF EXISTS ttl_nested_default_source_unread_compact;
CREATE TABLE ttl_nested_default_source_unread_compact (id UInt64, ts DateTime, n Nested(a UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO ttl_nested_default_source_unread_compact VALUES (0, '2000-01-01', [7, 8]);
INSERT INTO ttl_nested_default_source_unread_compact VALUES (1, '2000-01-01', [9]);
ALTER TABLE ttl_nested_default_source_unread_compact ADD COLUMN n.d Array(UInt8);
ALTER TABLE ttl_nested_default_source_unread_compact MODIFY COLUMN n.a Array(UInt64) DEFAULT [1, 2] TTL ts + INTERVAL 1 DAY;

OPTIMIZE TABLE ttl_nested_default_source_unread_compact FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'default source unread compact', id, n.a, length(n.d) FROM ttl_nested_default_source_unread_compact ORDER BY id;
SELECT 'default source unread compact stored', arraySort(groupUniqArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_default_source_unread_compact' AND active
    AND column IN ('id', 'ts', 'n.a', 'n.d');
DROP TABLE ttl_nested_default_source_unread_compact;

DROP TABLE IF EXISTS ttl_nested_default_source_index;
CREATE TABLE ttl_nested_default_source_index (id UInt64, ts DateTime, n Nested(a UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO ttl_nested_default_source_index VALUES (0, '2000-01-01', [7, 8]);
INSERT INTO ttl_nested_default_source_index VALUES (1, '2000-01-01', [9]);
ALTER TABLE ttl_nested_default_source_index ADD COLUMN n.d Array(UInt8);
ALTER TABLE ttl_nested_default_source_index ADD INDEX i length(n.d) TYPE minmax;
ALTER TABLE ttl_nested_default_source_index MODIFY COLUMN n.a Array(UInt64) DEFAULT [1, 2] TTL ts + INTERVAL 1 DAY;

OPTIMIZE TABLE ttl_nested_default_source_index FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'default source index', id, n.a, length(n.d) FROM ttl_nested_default_source_index ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'default source index stored', arraySort(groupUniqArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_default_source_index' AND active
    AND column IN ('id', 'ts', 'n.a', 'n.d');
DROP TABLE ttl_nested_default_source_index;

-- Every part has a text index over `n.d`, and `CLEAR COLUMN` then removes `n.d` from them. The parts no longer store
-- a column that their copies of the index were built over, so the merge builds the index from its expression, which
-- reads `n.d`, instead of merging their files.
DROP TABLE IF EXISTS ttl_nested_text_index;
CREATE TABLE ttl_nested_text_index (id UInt64, ts DateTime, n Nested(a UInt8))
ENGINE = MergeTree ORDER BY id TTL ts + INTERVAL 100 YEAR
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, index_granularity = 4,
    max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO ttl_nested_text_index SELECT number, '2000-01-01', [intDiv(number, 8)] FROM numbers(16);
INSERT INTO ttl_nested_text_index SELECT number + 16, '2000-01-01', [intDiv(number + 16, 8)] FROM numbers(16);
ALTER TABLE ttl_nested_text_index ADD COLUMN n.d Array(String);
ALTER TABLE ttl_nested_text_index ADD INDEX i arrayConcat(arrayMap(x -> toString(x), n.a), n.d) TYPE text(tokenizer = 'splitByNonAlpha');
ALTER TABLE ttl_nested_text_index MATERIALIZE INDEX i SETTINGS mutations_sync = 2;
ALTER TABLE ttl_nested_text_index CLEAR COLUMN n.d SETTINGS mutations_sync = 2;

OPTIMIZE TABLE ttl_nested_text_index FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'text index', count(), arraySort(groupArray(id)) FROM ttl_nested_text_index
WHERE has(arrayConcat(arrayMap(x -> toString(x), n.a), n.d), '2') SETTINGS force_data_skipping_indices = 'i';
SELECT 'text index parts', count(), countIf(secondary_indices_compressed_bytes > 0) FROM system.parts
WHERE database = currentDatabase() AND table = 'ttl_nested_text_index' AND active;
SELECT 'text index stored', arraySort(groupUniqArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_nested_text_index' AND active
    AND column IN ('id', 'ts', 'n.a', 'n.d');
DROP TABLE ttl_nested_text_index;
