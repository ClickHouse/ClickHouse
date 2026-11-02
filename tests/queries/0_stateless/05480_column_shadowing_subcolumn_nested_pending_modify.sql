-- A flattened `Nested` column recorded as missing in a part keeps its frozen default, converted to the current type
-- of the column while `MODIFY COLUMN` is not yet applied to the part.

DROP TABLE IF EXISTS t_nested_pending_modify;

CREATE TABLE t_nested_pending_modify (id UInt64, `n.x` Array(UInt64), `n.y` UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_nested_pending_modify (id) VALUES (1);

SYSTEM STOP MERGES t_nested_pending_modify;
ALTER TABLE t_nested_pending_modify MODIFY COLUMN `n.x` Array(String) SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT arrayPushBack(`n.x`, 'hello'), toTypeName(`n.x`), `n.x`.size0 FROM t_nested_pending_modify;
SELECT n.x, toTypeName(n.x) FROM t_nested_pending_modify;

SYSTEM START MERGES t_nested_pending_modify;
ALTER TABLE t_nested_pending_modify MODIFY COLUMN `n.y` String SETTINGS alter_sync = 2, mutations_sync = 2;

SELECT arrayPushBack(`n.x`, 'hello'), toTypeName(`n.x`), `n.y`, toTypeName(`n.y`) FROM t_nested_pending_modify;

DROP TABLE t_nested_pending_modify;
