-- Tags: no-fasttest
-- no-fasttest: needs the SZ3 library

-- The lossy-codec guard of `ALTER TABLE ... RECOMPRESS COLUMN` maps every column a dependent requires
-- back to the stored column that owns it. With a column `a` and a stored column `a.b Tuple(x Float64)`,
-- `a.b.x` is a subcolumn of `a.b`, not of `a`: the ownership must be resolved by type, not by the
-- first stored prefix, so dependents of `a.b.x` still block a lossy recompression of `a.b`.

SET allow_experimental_recompress_column = 1;
SET enable_sz3_codec = 1;
SET mutations_sync = 2;

-- A skip index over a subcolumn of the dotted column.
DROP TABLE IF EXISTS t_recompress_dotted_index;
CREATE TABLE t_recompress_dotted_index
(
    k UInt64, a Bool, `a.b` Tuple(x Float64),
    INDEX idx `a.b.x` TYPE set(10000) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
    min_level_for_full_part_storage = 0, index_granularity = 100;
INSERT INTO t_recompress_dotted_index SELECT number, number % 2, tuple(sin(number / 100.) * 100) FROM numbers(1000);
ALTER TABLE t_recompress_dotted_index MODIFY COLUMN `a.b` CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01));
ALTER TABLE t_recompress_dotted_index RECOMPRESS COLUMN `a.b`; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_recompress_dotted_index DROP INDEX idx;
ALTER TABLE t_recompress_dotted_index RECOMPRESS COLUMN `a.b`;
SELECT 'index dropped', count() FROM t_recompress_dotted_index;
DROP TABLE t_recompress_dotted_index;

-- A stored `MATERIALIZED` column computed from a subcolumn of the dotted column.
DROP TABLE IF EXISTS t_recompress_dotted_materialized;
CREATE TABLE t_recompress_dotted_materialized
(
    k UInt64, a Bool, `a.b` Tuple(x Float64), m Float64 MATERIALIZED `a.b.x`
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
    min_level_for_full_part_storage = 0;
INSERT INTO t_recompress_dotted_materialized SELECT number, number % 2, tuple(sin(number / 100.) * 100) FROM numbers(1000);
ALTER TABLE t_recompress_dotted_materialized MODIFY COLUMN `a.b` CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01));
ALTER TABLE t_recompress_dotted_materialized RECOMPRESS COLUMN `a.b`; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'materialized consistent', countIf(m = `a.b.x`) FROM t_recompress_dotted_materialized;
DROP TABLE t_recompress_dotted_materialized;
