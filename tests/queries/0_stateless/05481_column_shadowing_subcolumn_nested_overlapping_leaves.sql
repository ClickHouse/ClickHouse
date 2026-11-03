-- The flattened leaves `n.x` and `n.x.y` coexist, and a part records both as missing. The frozen default of a read of
-- `n.x.y.null` comes from the marker of the leaf `n.x.y`, not from the marker of the shorter leaf `n.x`.

DROP TABLE IF EXISTS t_nested_overlapping_leaves;

CREATE TABLE t_nested_overlapping_leaves (id UInt64, `n.x` Array(UInt64), `n.x.y` Array(Nullable(UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_nested_overlapping_leaves (id) VALUES (1);

SELECT `n.x`, `n.x.y`, `n.x.y.null`, `n.x.size0` FROM t_nested_overlapping_leaves;

DROP TABLE t_nested_overlapping_leaves;
