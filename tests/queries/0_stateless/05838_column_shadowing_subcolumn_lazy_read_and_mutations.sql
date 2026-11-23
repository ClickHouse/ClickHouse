-- A column of the table whose name is also the name of a subcolumn of another column (`a.size0` next to an
-- `Array` column `a`, `a.x` next to a `JSON` column `a`), in parts that do not hold it.

SELECT '-- a lazily read column is not served by a projection that stores only the other column';
-- The projection `p` stores `a` and not `a.size0`. It serves the filter and the order by `k`, and the lazy read
-- then reads its columns from the projection part: there, `a.size0` would be the sizes of `a`.
DROP TABLE IF EXISTS t_shadow_lazy;
CREATE TABLE t_shadow_lazy (id UInt64, k UInt64, a Array(UInt64), `a.size0` UInt64 DEFAULT 7,
    PROJECTION p (SELECT id, k, a ORDER BY k))
ENGINE = MergeTree ORDER BY id
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 1;

INSERT INTO t_shadow_lazy (id, k, a) SELECT number, 1000 - number, range(number % 3) FROM numbers(1000);

SET optimize_use_projections = 1, query_plan_optimize_lazy_materialization = 1,
    query_plan_max_limit_for_lazy_materialization = 100, optimize_move_to_prewhere = 0;

SELECT id, `a.size0` FROM t_shadow_lazy WHERE k IN (5, 6) ORDER BY k LIMIT 2;
SELECT id, a, `a.size0` FROM t_shadow_lazy WHERE k IN (5, 6) ORDER BY k LIMIT 2;
SELECT id, `a.size0` FROM t_shadow_lazy WHERE k IN (5, 6) ORDER BY k LIMIT 2
SETTINGS optimize_use_projections = 0, query_plan_optimize_lazy_materialization = 0;

-- The column `a` itself is still read lazily from the projection.
SELECT id, a FROM t_shadow_lazy WHERE k IN (5, 6) ORDER BY k LIMIT 2;

DROP TABLE t_shadow_lazy;

SELECT '-- mutations of a column that the part does not hold';
DROP TABLE IF EXISTS t_shadow_mutate;
CREATE TABLE t_shadow_mutate (id UInt64, a JSON, `a.x` UInt64 DEFAULT 7, b UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_shadow_mutate (id, a, b) VALUES (1, '{"x" : 11}', 1), (2, '{"x" : 22}', 2);
SELECT id, a, `a.x`, b FROM t_shadow_mutate ORDER BY id;

ALTER TABLE t_shadow_mutate MODIFY COLUMN `a.x` UInt32 DEFAULT 7 SETTINGS mutations_sync = 2;
SELECT id, a, `a.x`, toTypeName(`a.x`), b FROM t_shadow_mutate ORDER BY id;

ALTER TABLE t_shadow_mutate MATERIALIZE COLUMN `a.x` SETTINGS mutations_sync = 2;
SELECT id, a, `a.x`, b FROM t_shadow_mutate ORDER BY id;

ALTER TABLE t_shadow_mutate UPDATE `a.x` = 9 WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT id, a, `a.x`, b FROM t_shadow_mutate ORDER BY id;

ALTER TABLE t_shadow_mutate CLEAR COLUMN `a.x` SETTINGS mutations_sync = 2;
SELECT id, a, `a.x`, b FROM t_shadow_mutate ORDER BY id;

DROP TABLE t_shadow_mutate;

SELECT '-- a projection computed from the recorded default of a missing column stays usable';
DROP TABLE IF EXISTS t_shadow_marker;
CREATE TABLE t_shadow_marker (id UInt64, a Array(UInt64), `a.size0` UInt64,
    PROJECTION p (SELECT sum(`a.size0`)))
ENGINE = MergeTree ORDER BY id
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_shadow_marker (id, a) VALUES (1, [1, 2, 3]), (2, [4]);
INSERT INTO t_shadow_marker (id, a) VALUES (3, [5, 6]);
SELECT sum(`a.size0`) FROM t_shadow_marker SETTINGS optimize_use_projections = 0;
SELECT sum(`a.size0`) FROM t_shadow_marker SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
OPTIMIZE TABLE t_shadow_marker FINAL;
SELECT sum(`a.size0`) FROM t_shadow_marker SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
ALTER TABLE t_shadow_marker UPDATE a = [7] WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT sum(`a.size0`) FROM t_shadow_marker SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
DROP TABLE t_shadow_marker;

SELECT '-- a skip index over a subcolumn of a missing column is not answered by a path of the JSON column';
DROP TABLE IF EXISTS t_shadow_index;
CREATE TABLE t_shadow_index (id UInt64, a JSON, `a.x` Tuple(y UInt64) DEFAULT tuple(7),
    INDEX i `a.x.y` TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 1;
INSERT INTO t_shadow_index (id, a) VALUES (1, '{"x" : {"y" : 11}}'), (2, '{"x" : {"y" : 22}}');
SELECT id, `a.x.y` FROM t_shadow_index ORDER BY id;
SELECT count() FROM t_shadow_index WHERE `a.x.y` = 7 SETTINGS use_skip_indexes = 1, optimize_move_to_prewhere = 0;
SELECT count() FROM t_shadow_index WHERE `a.x.y` = 7 SETTINGS use_skip_indexes = 0, optimize_move_to_prewhere = 0;
SELECT count() FROM t_shadow_index WHERE `a.x.y` = 11 SETTINGS use_skip_indexes = 1, optimize_move_to_prewhere = 0;
DROP TABLE t_shadow_index;

SELECT '-- a skip index is computed on insert from the subcolumn that the table gives the name';
-- `a.x.y` is the element `y` of the column `a.x`, not the path `x.y` of the `JSON` column `a`.
DROP TABLE IF EXISTS t_shadow_index_insert;
CREATE TABLE t_shadow_index_insert (id UInt64, a JSON, `a.x` Tuple(y Dynamic), INDEX i toUInt64(`a.x.y`) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_shadow_index_insert VALUES (1, '{"x" : {"y" : 11}}', tuple(5::UInt64)), (2, '{"x" : {"y" : 22}}', tuple(6::UInt64));
SELECT id, `a.x.y` FROM t_shadow_index_insert ORDER BY id;
SELECT count() FROM t_shadow_index_insert WHERE toUInt64(`a.x.y`) = 5 SETTINGS use_skip_indexes = 1, optimize_move_to_prewhere = 0;
SELECT count() FROM t_shadow_index_insert WHERE toUInt64(`a.x.y`) = 5 SETTINGS use_skip_indexes = 0, optimize_move_to_prewhere = 0;
SELECT count() FROM t_shadow_index_insert WHERE toUInt64(`a.x.y`) = 11 SETTINGS use_skip_indexes = 1, optimize_move_to_prewhere = 0;
DROP TABLE t_shadow_index_insert;

-- The same for a sorting key and a partition key.
DROP TABLE IF EXISTS t_shadow_key_insert;
CREATE TABLE t_shadow_key_insert (id UInt64, a JSON, `a.x` Tuple(y UInt64))
ENGINE = MergeTree ORDER BY `a.x.y` PARTITION BY `a.x.y` % 2;
INSERT INTO t_shadow_key_insert VALUES (1, '{"x" : {"y" : 11}}', tuple(6)), (2, '{"x" : {"y" : 22}}', tuple(5));
SELECT id, `a.x.y`, _partition_id FROM t_shadow_key_insert ORDER BY `a.x.y`;
SELECT count() FROM t_shadow_key_insert WHERE `a.x.y` = 5;
DROP TABLE t_shadow_key_insert;
