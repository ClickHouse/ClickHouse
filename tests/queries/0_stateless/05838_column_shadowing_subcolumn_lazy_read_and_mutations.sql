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
