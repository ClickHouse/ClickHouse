-- A MATERIALIZED column (`m = hex(x)`) is recalculated when ALTER MODIFY COLUMN changes the type of its
-- source, even when the source is stored only as a missing-column marker (skip_empty_columns_on_insert):
-- the old type is resolved from the part's serialization infos, so the type change is detected.

DROP TABLE IF EXISTS t_materialized_missing_column;

CREATE TABLE t_materialized_missing_column (k UInt32, x FixedString(3), m String MATERIALIZED hex(x))
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';

INSERT INTO t_materialized_missing_column (k, x) VALUES (1, '');

-- FixedString(3) -> String strips the trailing zero bytes, so hex(x) changes from '000000' to ''.
ALTER TABLE t_materialized_missing_column MODIFY COLUMN x String SETTINGS mutations_sync = 2;

SELECT m = hex(x) FROM t_materialized_missing_column;

DROP TABLE t_materialized_missing_column;
