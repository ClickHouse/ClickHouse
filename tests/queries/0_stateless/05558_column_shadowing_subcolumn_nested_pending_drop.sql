-- A pending `DROP COLUMN` or `CLEAR COLUMN` of a flattened `Nested` column recorded as missing in a part invalidates
-- its frozen default: the column is read with its current default, as after the mutation is applied.

DROP TABLE IF EXISTS t_nested_pending_drop;

CREATE TABLE t_nested_pending_drop (id UInt64, `n.x` Array(UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_level_for_wide_part = 0;

INSERT INTO t_nested_pending_drop (id) VALUES (1);

SYSTEM STOP MERGES t_nested_pending_drop;
ALTER TABLE t_nested_pending_drop DROP COLUMN `n.x` SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t_nested_pending_drop ADD COLUMN `n.x` Array(UInt64) DEFAULT [7];

SELECT `n.x`, `n.x.size0` FROM t_nested_pending_drop;

DROP TABLE t_nested_pending_drop;

CREATE TABLE t_nested_pending_drop (id UInt64, `n.x` Array(UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_level_for_wide_part = 0;

INSERT INTO t_nested_pending_drop (id) VALUES (1);

SYSTEM STOP MERGES t_nested_pending_drop;
ALTER TABLE t_nested_pending_drop MODIFY COLUMN `n.x` Array(UInt64) DEFAULT [7];
ALTER TABLE t_nested_pending_drop CLEAR COLUMN `n.x` SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT `n.x`, `n.x.size0` FROM t_nested_pending_drop;

DROP TABLE t_nested_pending_drop;
