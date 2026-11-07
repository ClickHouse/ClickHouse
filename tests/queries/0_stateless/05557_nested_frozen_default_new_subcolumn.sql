-- The frozen default of a flattened `Nested` column recorded as missing is converted to the current
-- type of the column before its subcolumn is extracted: the subcolumn may exist only in the new type.

DROP TABLE IF EXISTS t_nested_frozen_subcolumn;

CREATE TABLE t_nested_frozen_subcolumn (id UInt64, `n.x` Array(UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_nested_frozen_subcolumn (id) VALUES (1);
SYSTEM STOP MERGES t_nested_frozen_subcolumn;
ALTER TABLE t_nested_frozen_subcolumn MODIFY COLUMN `n.x` Array(Nullable(UInt64)) SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT `n.x.null` FROM t_nested_frozen_subcolumn;
SELECT `n.x.size0` FROM t_nested_frozen_subcolumn;
SELECT `n.x` FROM t_nested_frozen_subcolumn;

DROP TABLE t_nested_frozen_subcolumn;
