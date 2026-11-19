-- A skip index defined on an expression over a subcolumn (`t.a + 1`) is rebuilt when the parent column
-- is mutated on a wide part. The recalculated index keeps its stored expression name, so the mutation
-- does not fail with NOT_FOUND_COLUMN_IN_BLOCK and the index does not serve stale granules.

DROP TABLE IF EXISTS t_expr_subcolumn_index;

CREATE TABLE t_expr_subcolumn_index (id UInt32, t Tuple(a Int32, b String), INDEX idx (t.a + 1) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_expr_subcolumn_index SELECT number, (number, 'x') FROM numbers(16);

ALTER TABLE t_expr_subcolumn_index UPDATE t = tuple(t.a, t.b) WHERE 1 SETTINGS mutations_sync = 2;

SELECT count() FROM t_expr_subcolumn_index WHERE t.a + 1 >= 2;
SELECT count() FROM t_expr_subcolumn_index WHERE t.a + 1 >= 2 SETTINGS use_skip_indexes = 0;

ALTER TABLE t_expr_subcolumn_index MODIFY COLUMN t Tuple(a Float32, b String) SETTINGS mutations_sync = 2;

SELECT count() FROM t_expr_subcolumn_index WHERE t.a + 1 >= 2;
SELECT count() FROM t_expr_subcolumn_index WHERE t.a + 1 >= 2 SETTINGS use_skip_indexes = 0;

DROP TABLE t_expr_subcolumn_index;
