-- Tags: no-fasttest
-- no-fasttest: needs the SZ3 library

-- A lossy codec can also be declared for a subcolumn only (`subcolumn_codecs`), in which case the column
-- has no column codec. `ALTER TABLE ... RECOMPRESS COLUMN` must still reject the recompression when a
-- stored dependent (a `MATERIALIZED` column, a skip index) reads the values the lossy codec would change.

SET enable_subcolumn_codecs = 1;
SET enable_sz3_codec = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_recompress_lossy_subcolumn_materialized;
CREATE TABLE t_recompress_lossy_subcolumn_materialized
(
    k UInt64,
    p Tuple(v Float64),
    m Float64 MATERIALIZED p.v
)
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_recompress_lossy_subcolumn_materialized (k, p) SELECT number, tuple(sin(number / 100.) * 100) FROM numbers(1000);

ALTER TABLE t_recompress_lossy_subcolumn_materialized MODIFY COLUMN p.v CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01));
ALTER TABLE t_recompress_lossy_subcolumn_materialized RECOMPRESS COLUMN p; -- { serverError SUPPORT_IS_DISABLED }
SELECT 'materialized dependent', countIf(m != p.v) FROM t_recompress_lossy_subcolumn_materialized;

DROP TABLE t_recompress_lossy_subcolumn_materialized;

DROP TABLE IF EXISTS t_recompress_lossy_subcolumn_index;
CREATE TABLE t_recompress_lossy_subcolumn_index
(
    k UInt64,
    p Tuple(v Float64),
    INDEX idx p.v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_recompress_lossy_subcolumn_index SELECT number, tuple(sin(number / 100.) * 100) FROM numbers(1000);

ALTER TABLE t_recompress_lossy_subcolumn_index MODIFY COLUMN p.v CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01));
ALTER TABLE t_recompress_lossy_subcolumn_index RECOMPRESS COLUMN p; -- { serverError SUPPORT_IS_DISABLED }

-- Without the dependent the recompression is allowed.
ALTER TABLE t_recompress_lossy_subcolumn_index DROP INDEX idx;
ALTER TABLE t_recompress_lossy_subcolumn_index RECOMPRESS COLUMN p;
SELECT 'no dependent', count(), max(abs(p.v - sin(k / 100.) * 100)) <= 0.01 FROM t_recompress_lossy_subcolumn_index;

DROP TABLE t_recompress_lossy_subcolumn_index;
