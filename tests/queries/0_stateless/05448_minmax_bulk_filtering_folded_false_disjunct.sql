-- Tags: no-parallel-replicas
-- no-parallel-replicas: `max_rows_to_read` checks the number of granules kept by skip indexes.

-- An atom folded to `ALWAYS_FALSE` for a multi-column `minmax` index (`a = 0.5` with `a UInt8`)
-- under an `OR` reports `false` into the partial-disjunction bitset. Bulk filtering must not be
-- used for such an index, otherwise the bit stays `true` and the disjunction is not pruned.

DROP TABLE IF EXISTS t_minmax_folded_false;

CREATE TABLE t_minmax_folded_false
(
    a UInt8, b UInt8, c UInt8,
    INDEX idx_ac (a, c) TYPE minmax GRANULARITY 1,
    INDEX idx_b b TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_minmax_folded_false VALUES (0, 0, 1), (0, 0, 1), (0, 0, 0), (0, 1, 1);

SELECT count() FROM t_minmax_folded_false WHERE c = 1 AND (a = 0.5 OR b = 1)
SETTINGS use_minmax_index_bulk_filtering = 0, secondary_indices_enable_bulk_filtering = 1,
    use_skip_indexes_on_data_read = 0, use_skip_indexes_for_disjunctions = 1, max_rows_to_read = 1;

SELECT count() FROM t_minmax_folded_false WHERE c = 1 AND (a = 0.5 OR b = 1)
SETTINGS use_minmax_index_bulk_filtering = 1, secondary_indices_enable_bulk_filtering = 1,
    use_skip_indexes_on_data_read = 0, use_skip_indexes_for_disjunctions = 1, max_rows_to_read = 1;

DROP TABLE t_minmax_folded_false;
