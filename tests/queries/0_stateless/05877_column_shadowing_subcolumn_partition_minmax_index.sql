-- The partition min/max index of a part is built from the column that the table gives the name, as the partition
-- key itself: next to a `Map` column `a` and a `Tuple` column `a.key_x`, the subcolumn `a.key_x.y` is the element
-- `y` of `a.key_x`, not the value of the key `x.y` of `a`. Otherwise the index holds the bounds of another column
-- and a condition on the subcolumn drops the part with the matching rows.

DROP TABLE IF EXISTS t_shadow_minmax;
CREATE TABLE t_shadow_minmax (id UInt64, a Map(String, UInt64), `a.key_x` Tuple(y UInt64))
ENGINE = MergeTree ORDER BY id PARTITION BY `a.key_x.y` % 2
SETTINGS share_nested_offsets = 0;

SELECT '-- INSERT';
INSERT INTO t_shadow_minmax VALUES (1, map('x.y', 11), tuple(6));
SELECT partition FROM system.parts WHERE database = currentDatabase() AND table = 't_shadow_minmax' AND active;
SELECT count() FROM t_shadow_minmax WHERE `a.key_x.y` = 6;
SELECT countIf(`a.key_x.y` = 6) FROM t_shadow_minmax;
SELECT count() FROM t_shadow_minmax WHERE `a.key_x.y` = 11;

SELECT '-- mutation';
ALTER TABLE t_shadow_minmax UPDATE a = map('x.y', 21) WHERE 1 SETTINGS mutations_sync = 2;
SELECT count() FROM t_shadow_minmax WHERE `a.key_x.y` = 6;
SELECT countIf(`a.key_x.y` = 6) FROM t_shadow_minmax;

SELECT '-- merge';
INSERT INTO t_shadow_minmax VALUES (2, map('x.y', 13), tuple(8));
OPTIMIZE TABLE t_shadow_minmax FINAL;
SELECT count() FROM t_shadow_minmax WHERE `a.key_x.y` = 8;
SELECT countIf(`a.key_x.y` = 8) FROM t_shadow_minmax;
SELECT count() FROM t_shadow_minmax WHERE `a.key_x.y` IN (13, 21);

SELECT '-- a merge that may reduce rows';
DROP TABLE IF EXISTS t_shadow_minmax_replacing;
CREATE TABLE t_shadow_minmax_replacing (id UInt64, a Map(String, UInt64), `a.key_x` Tuple(y UInt64))
ENGINE = ReplacingMergeTree ORDER BY id PARTITION BY `a.key_x.y` % 2
SETTINGS share_nested_offsets = 0;
INSERT INTO t_shadow_minmax_replacing VALUES (1, map('x.y', 11), tuple(6));
INSERT INTO t_shadow_minmax_replacing VALUES (1, map('x.y', 13), tuple(8));
OPTIMIZE TABLE t_shadow_minmax_replacing FINAL;
SELECT count() FROM t_shadow_minmax_replacing WHERE `a.key_x.y` = 8;
SELECT countIf(`a.key_x.y` = 8) FROM t_shadow_minmax_replacing;

DROP TABLE t_shadow_minmax;
DROP TABLE t_shadow_minmax_replacing;
