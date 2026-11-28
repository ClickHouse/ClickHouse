-- Next to a `JSON` column `a` and a `Tuple` column `a.x`, the subcolumn `a.x.y` is the element `y` of `a.x`, not the
-- path `x.y` of `a`. The key of a projection, the sorting key and a skip index over it are computed from `a.x`
-- on insert, in a merge (horizontal and vertical) and in a mutation, as queries read it.
-- The values of the path `x.y` are 11, 22, 33 and the values of the element are 55, 44, 66.

SELECT '-- key of a normal projection';
DROP TABLE IF EXISTS t_shadow_projection_key;
CREATE TABLE t_shadow_projection_key (id UInt64, a JSON, `a.x` Tuple(y Dynamic),
    PROJECTION p (SELECT id, a, `a.x` ORDER BY toUInt64(`a.x.y`)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_shadow_projection_key VALUES (1, '{"x" : {"y" : 11}}', tuple(55::UInt64)), (2, '{"x" : {"y" : 22}}', tuple(44::UInt64));
INSERT INTO t_shadow_projection_key VALUES (3, '{"x" : {"y" : 33}}', tuple(66::UInt64));

SELECT count() FROM t_shadow_projection_key WHERE toUInt64(`a.x.y`) = 44 SETTINGS optimize_use_projections = 0;
SELECT count() FROM t_shadow_projection_key WHERE toUInt64(`a.x.y`) = 44 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT count() FROM t_shadow_projection_key WHERE toUInt64(`a.x.y`) = 22 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT id, `a.x.y` FROM t_shadow_projection_key WHERE toUInt64(`a.x.y`) >= 44 ORDER BY toUInt64(`a.x.y`) SETTINGS optimize_use_projections = 0;
SELECT id, `a.x.y` FROM t_shadow_projection_key WHERE toUInt64(`a.x.y`) >= 44 ORDER BY toUInt64(`a.x.y`) SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

OPTIMIZE TABLE t_shadow_projection_key FINAL;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_shadow_projection_key' AND active;
SELECT count() FROM t_shadow_projection_key WHERE toUInt64(`a.x.y`) = 44 SETTINGS optimize_use_projections = 0;
SELECT count() FROM t_shadow_projection_key WHERE toUInt64(`a.x.y`) = 44 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT count() FROM t_shadow_projection_key WHERE toUInt64(`a.x.y`) = 22 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT id, `a.x.y` FROM t_shadow_projection_key WHERE toUInt64(`a.x.y`) >= 44 ORDER BY toUInt64(`a.x.y`) SETTINGS optimize_use_projections = 0;
SELECT id, `a.x.y` FROM t_shadow_projection_key WHERE toUInt64(`a.x.y`) >= 44 ORDER BY toUInt64(`a.x.y`) SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
DROP TABLE t_shadow_projection_key;

SELECT '-- sorting key and skip index, horizontal merge';
DROP TABLE IF EXISTS t_shadow_keys_horizontal;
CREATE TABLE t_shadow_keys_horizontal (id UInt64, a JSON, `a.x` Tuple(y Dynamic), INDEX i toUInt64(`a.x.y`) + 1 TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY toUInt64(`a.x.y`)
SETTINGS index_granularity = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO t_shadow_keys_horizontal VALUES (1, '{"x" : {"y" : 11}}', tuple(55::UInt64)), (2, '{"x" : {"y" : 22}}', tuple(44::UInt64));
INSERT INTO t_shadow_keys_horizontal VALUES (3, '{"x" : {"y" : 33}}', tuple(66::UInt64));
OPTIMIZE TABLE t_shadow_keys_horizontal FINAL;
SELECT id FROM t_shadow_keys_horizontal ORDER BY toUInt64(`a.x.y`) SETTINGS optimize_read_in_order = 1;
SELECT count() FROM t_shadow_keys_horizontal WHERE toUInt64(`a.x.y`) = 44;
SELECT count() FROM t_shadow_keys_horizontal WHERE toUInt64(`a.x.y`) = 22;
SELECT count() FROM t_shadow_keys_horizontal WHERE toUInt64(`a.x.y`) + 1 = 45 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_shadow_keys_horizontal WHERE toUInt64(`a.x.y`) + 1 = 45 SETTINGS use_skip_indexes = 0;
DROP TABLE t_shadow_keys_horizontal;

SELECT '-- sorting key and skip index, vertical merge';
DROP TABLE IF EXISTS t_shadow_keys_vertical;
CREATE TABLE t_shadow_keys_vertical (id UInt64, a JSON, `a.x` Tuple(y Dynamic), INDEX i toUInt64(`a.x.y`) + 1 TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY toUInt64(`a.x.y`)
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1;
INSERT INTO t_shadow_keys_vertical VALUES (1, '{"x" : {"y" : 11}}', tuple(55::UInt64)), (2, '{"x" : {"y" : 22}}', tuple(44::UInt64));
INSERT INTO t_shadow_keys_vertical VALUES (3, '{"x" : {"y" : 33}}', tuple(66::UInt64));
OPTIMIZE TABLE t_shadow_keys_vertical FINAL;
SELECT id FROM t_shadow_keys_vertical ORDER BY toUInt64(`a.x.y`) SETTINGS optimize_read_in_order = 1;
SELECT count() FROM t_shadow_keys_vertical WHERE toUInt64(`a.x.y`) = 44;
SELECT count() FROM t_shadow_keys_vertical WHERE toUInt64(`a.x.y`) = 22;
SELECT count() FROM t_shadow_keys_vertical WHERE toUInt64(`a.x.y`) + 1 = 45 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_shadow_keys_vertical WHERE toUInt64(`a.x.y`) + 1 = 45 SETTINGS use_skip_indexes = 0;
DROP TABLE t_shadow_keys_vertical;

SELECT '-- skip index recalculated by a mutation';
DROP TABLE IF EXISTS t_shadow_index_mutation;
CREATE TABLE t_shadow_index_mutation (id UInt64, a JSON, `a.x` Tuple(y Dynamic), INDEX i toUInt64(`a.x.y`) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, packed_skip_index_max_bytes = 1048576;
-- The index is kept in the packed archive, so that the mutation recalculates it. An index in a file of its own is
-- not recalculated when the mutation updates the column of the subcolumn it is built on, with or without another
-- column of that name: https://github.com/ClickHouse/ClickHouse/issues/128280
INSERT INTO t_shadow_index_mutation VALUES (1, '{"x" : {"y" : 11}}', tuple(55::UInt64)), (2, '{"x" : {"y" : 22}}', tuple(44::UInt64));
ALTER TABLE t_shadow_index_mutation UPDATE `a.x` = tuple(77::UInt64) WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT id, `a.x.y` FROM t_shadow_index_mutation ORDER BY id;
SELECT count() FROM t_shadow_index_mutation WHERE toUInt64(`a.x.y`) = 77 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_shadow_index_mutation WHERE toUInt64(`a.x.y`) = 77 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_shadow_index_mutation WHERE toUInt64(`a.x.y`) = 11 SETTINGS use_skip_indexes = 1;
DROP TABLE t_shadow_index_mutation;
