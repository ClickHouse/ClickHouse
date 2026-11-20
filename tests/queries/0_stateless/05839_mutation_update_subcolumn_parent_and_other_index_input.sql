-- A skip index over a subcolumn and another column (`t.a + y`) is rebuilt from the updated parent `t` when one
-- UPDATE changes both inputs, not from the subcolumn of the source part.

DROP TABLE IF EXISTS t_idx_both_inputs;

CREATE TABLE t_idx_both_inputs (id UInt64, t Tuple(a Int32), y Int32, s String, INDEX idx (t.a + y) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 1;

INSERT INTO t_idx_both_inputs SELECT number, tuple(toInt32(0)), 0, 'payload' FROM numbers(16);

ALTER TABLE t_idx_both_inputs UPDATE t = tuple(100), y = 1 WHERE 1 SETTINGS mutations_sync = 2;

SELECT count() FROM t_idx_both_inputs WHERE t.a + y > 50 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_idx_both_inputs WHERE t.a + y > 50 SETTINGS use_skip_indexes = 0;

DROP TABLE t_idx_both_inputs;
