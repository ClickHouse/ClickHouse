-- `MODIFY COLUMN IF EXISTS` names an exact column: a Nested parent is not one.
-- `CLEAR COLUMN` keeps the column, so it keeps `auto_minmax_index_x`.

DROP TABLE IF EXISTS t;

CREATE TABLE t (id UInt8) ENGINE = MergeTree ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 1;

ALTER TABLE t ADD COLUMN n Nested(a UInt8), MODIFY COLUMN IF EXISTS n COMMENT 'c';
ALTER TABLE t ADD COLUMN x UInt8, CLEAR COLUMN IF EXISTS x;

DESC TABLE t;
SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't' AND name = 'auto_minmax_index_x';

DROP TABLE t;
