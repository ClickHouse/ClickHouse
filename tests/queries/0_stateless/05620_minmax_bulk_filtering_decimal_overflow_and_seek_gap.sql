-- Bulk minmax filtering must agree with the scalar evaluation of the index.

SET secondary_indices_enable_bulk_filtering = 1, use_skip_indexes_on_data_read = 0;

-- A comparison that rescales a decimal must not throw `DECIMAL_OVERFLOW` where the scalar index prunes the part.
DROP TABLE IF EXISTS t_minmax_dt64;
CREATE TABLE t_minmax_dt64
(
    t DateTime64(9),
    INDEX idx_t t TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_minmax_dt64 VALUES (1), (2);

SELECT 'overflow scalar', count() FROM t_minmax_dt64 WHERE t > toUInt64('18446744073709551615') SETTINGS use_minmax_index_bulk_filtering = 0;
SELECT 'overflow bulk', count() FROM t_minmax_dt64 WHERE t > toUInt64('18446744073709551615') SETTINGS use_minmax_index_bulk_filtering = 1;

DROP TABLE t_minmax_dt64;

-- A mark rejected by a bulk index must stay excluded when a later index merges ranges across a seek gap.
DROP TABLE IF EXISTS t_minmax_gap;
CREATE TABLE t_minmax_gap
(
    t UInt8, v UInt8, w UInt8,
    INDEX idx_t t TYPE minmax GRANULARITY 1,
    INDEX idx_v v TYPE minmax GRANULARITY 1,
    INDEX idx_w w TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_minmax_gap VALUES (1, 0, 0), (0, 0, 1), (0, 0, 1), (1, 0, 0);

SET use_skip_indexes_for_disjunctions = 1, use_statistics_for_part_pruning = 0, merge_tree_min_rows_for_seek = 100;

SELECT 'gap scalar', count() FROM t_minmax_gap WHERE t = 1 AND (v = 1 OR w = 1)
SETTINGS use_minmax_index_bulk_filtering = 0, max_rows_to_read = 1;
SELECT 'gap bulk', count() FROM t_minmax_gap WHERE t = 1 AND (v = 1 OR w = 1)
SETTINGS use_minmax_index_bulk_filtering = 1, max_rows_to_read = 1;

DROP TABLE t_minmax_gap;
