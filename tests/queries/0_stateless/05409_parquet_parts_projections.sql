-- Tags: no-fasttest
-- Tag no-fasttest: Depends on Parquet

-- A projection in Parquet format inside data parts in another format, and vice versa.

DROP TABLE IF EXISTS t_projection_parquet;
DROP TABLE IF EXISTS t_parquet_projection;

CREATE TABLE t_projection_parquet
(
    key UInt64,
    value String,
    PROJECTION p (SELECT * ORDER BY value) WITH SETTINGS (min_rows_for_parquet_part = 1, parquet_row_group_granularity_rows = 100)
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_parquet_part = 0;

INSERT INTO t_projection_parquet SELECT number, toString(number) FROM numbers(1000);

SELECT 'table in Wide, projection in Parquet';
SELECT part_type, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_projection_parquet' AND active;
SELECT name, part_type, rows, marks FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_projection_parquet' AND active;
SELECT key FROM t_projection_parquet WHERE value = '555' SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT count() FROM t_projection_parquet WHERE value LIKE '12%' SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

INSERT INTO t_projection_parquet SELECT number + 1000, toString(number + 1000) FROM numbers(1000);
OPTIMIZE TABLE t_projection_parquet FINAL;
-- The sizes of row groups after a merge depend on the sizes of blocks, but they are limited by the setting of the projection.
SELECT name, part_type, rows, marks > 10 FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_projection_parquet' AND active;
SELECT key FROM t_projection_parquet WHERE value = '1555' SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

CREATE TABLE t_parquet_projection
(
    key UInt64,
    value String,
    PROJECTION p (SELECT * ORDER BY value) WITH SETTINGS (min_rows_for_parquet_part = 0, min_bytes_for_wide_part = 1000000000)
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_rows_for_parquet_part = 1, parquet_row_group_granularity_rows = 100;

INSERT INTO t_parquet_projection SELECT number, toString(number) FROM numbers(1000);

SELECT 'table in Parquet, projection in Compact';
SELECT part_type, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_projection' AND active;
SELECT name, part_type, rows FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_parquet_projection' AND active;
SELECT key FROM t_parquet_projection WHERE value = '555' SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT key FROM t_parquet_projection WHERE key = 555 SETTINGS optimize_use_projections = 0;

DROP TABLE t_projection_parquet;
DROP TABLE t_parquet_projection;
