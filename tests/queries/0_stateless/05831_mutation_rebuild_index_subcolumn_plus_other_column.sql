-- A skip index over a subcolumn and a second column (`t.a + y`) is rebuilt when the parent of the
-- subcolumn is mutated on a wide part. The recalculation must read the index's other input (`y`) too,
-- not only the mutated column, so the mutation does not fail with NOT_FOUND_COLUMN_IN_BLOCK.

DROP TABLE IF EXISTS t_index_subcolumn_plus_other;

CREATE TABLE t_index_subcolumn_plus_other (id UInt32, t Tuple(a Int32, b String), y Int32, INDEX idx (t.a + y) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_index_subcolumn_plus_other SELECT number, (number, 'x'), number FROM numbers(16);

ALTER TABLE t_index_subcolumn_plus_other UPDATE t = tuple(t.a + 1, t.b) WHERE 1 SETTINGS mutations_sync = 2;

SELECT count() FROM t_index_subcolumn_plus_other WHERE t.a + y >= 2;
SELECT count() FROM t_index_subcolumn_plus_other WHERE t.a + y >= 2 SETTINGS use_skip_indexes = 0;

DROP TABLE t_index_subcolumn_plus_other;
