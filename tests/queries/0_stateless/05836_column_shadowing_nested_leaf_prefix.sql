-- A column `a.x.y` of type `Array` is read as the leaf `x.y` of a `Nested` column `a`. A part that does not hold it,
-- but holds a `JSON` column `a` and a column `a.x`, must give its default, not the path `x.y` of `a`: the stored
-- column `a.x` is another column of the table, not the leaf.

DROP TABLE IF EXISTS t_shadow_leaf;

CREATE TABLE t_shadow_leaf (id UInt64, a JSON, `a.x` UInt64, `a.x.y` Array(UInt64))
ENGINE = MergeTree ORDER BY length(`a.x.y`)
SETTINGS index_granularity = 1, share_nested_offsets = 1,
    skip_empty_columns_on_insert = 1,
    serialization_info_version = 'with_missing_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_level_for_wide_part = 0;

INSERT INTO t_shadow_leaf (id, a, `a.x`) VALUES (1, '{"x":{"y":[11,22]}}', 5);

SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_shadow_leaf' AND active ORDER BY name;
SELECT id, `a.x`, `a.x.y`, `a.x.y.size0`, toJSONString(a) FROM t_shadow_leaf ORDER BY id;
SELECT count() FROM t_shadow_leaf WHERE length(`a.x.y`) = 0;
SELECT countIf(length(`a.x.y`) = 0) FROM t_shadow_leaf;

DROP TABLE t_shadow_leaf;
