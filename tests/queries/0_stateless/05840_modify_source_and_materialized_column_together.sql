-- One ALTER that changes the type of a source and of a MATERIALIZED column computed from it recalculates that
-- column from the converted source (not just casting its stored value), and then the columns computed from it.

DROP TABLE IF EXISTS t_modify_source_and_materialized;

CREATE TABLE t_modify_source_and_materialized (x Int64, m1 Int64 MATERIALIZED x % 1000, m2 Int64 MATERIALIZED m1 * 2, s String)
ENGINE = MergeTree ORDER BY s
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_modify_source_and_materialized (x, s) VALUES (5000000001, 'payload');

ALTER TABLE t_modify_source_and_materialized MODIFY COLUMN x Int32, MODIFY COLUMN m1 Int32 SETTINGS mutations_sync = 2;

SELECT x, m1, x % 1000, m2 FROM t_modify_source_and_materialized;

DROP TABLE t_modify_source_and_materialized;
