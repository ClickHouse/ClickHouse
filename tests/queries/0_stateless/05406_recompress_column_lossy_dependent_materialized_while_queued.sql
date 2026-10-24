-- Tags: no-fasttest, no-shared-merge-tree
-- no-fasttest: needs the SZ3 library
-- no-shared-merge-tree: the test uses `SYSTEM STOP MERGES` to keep the mutations of a plain local
-- MergeTree table queued, so that they could be executed by one mutation task.

-- A lossy `RECOMPRESS COLUMN` queued before a skip index, a projection or a `MATERIALIZED` column
-- depending on the column is added, followed by the materialization of that dependent. The part
-- does not have the dependent, so the recompression does not reject it, but if both mutations ran in
-- one task, the task would build the dependent from the values as they were before the
-- recompression. `RECOMPRESS COLUMN` is not combined with other mutations, so the materialization
-- reads the recompressed values and the dependent stays consistent with the base part.

SET enable_sz3_codec = 1;

DROP TABLE IF EXISTS t_recompress_index;
DROP TABLE IF EXISTS t_recompress_projection;
DROP TABLE IF EXISTS t_recompress_materialized;
DROP TABLE IF EXISTS expected_index;
DROP TABLE IF EXISTS expected_projection;

CREATE TABLE t_recompress_index (key UInt64, val Float64 CODEC(ZSTD(1)))
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 100;
INSERT INTO t_recompress_index SELECT number, sin(number / 100.) * 100 FROM numbers(1000);
ALTER TABLE t_recompress_index MODIFY COLUMN val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01));
SYSTEM STOP MERGES t_recompress_index;
ALTER TABLE t_recompress_index RECOMPRESS COLUMN val SETTINGS mutations_sync = 0;
ALTER TABLE t_recompress_index ADD INDEX idx val TYPE set(10000) GRANULARITY 1;
ALTER TABLE t_recompress_index MATERIALIZE INDEX idx SETTINGS mutations_sync = 0;
SYSTEM START MERGES t_recompress_index;
ALTER TABLE t_recompress_index MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;

SELECT 'index';
CREATE TABLE expected_index ENGINE = Memory AS SELECT val FROM t_recompress_index SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_recompress_index WHERE val IN (SELECT val FROM expected_index)
SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_recompress_index WHERE val IN (SELECT val FROM expected_index)
SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'idx';

CREATE TABLE t_recompress_projection (key UInt64, val Float64 CODEC(ZSTD(1)))
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 100;
INSERT INTO t_recompress_projection SELECT number, sin(number / 100.) * 100 FROM numbers(1000);
ALTER TABLE t_recompress_projection MODIFY COLUMN val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01));
SYSTEM STOP MERGES t_recompress_projection;
ALTER TABLE t_recompress_projection RECOMPRESS COLUMN val SETTINGS mutations_sync = 0;
ALTER TABLE t_recompress_projection ADD PROJECTION p (SELECT key, val ORDER BY val);
ALTER TABLE t_recompress_projection MATERIALIZE PROJECTION p SETTINGS mutations_sync = 0;
SYSTEM START MERGES t_recompress_projection;
ALTER TABLE t_recompress_projection MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

SELECT 'projection';
CREATE TABLE expected_projection ENGINE = Memory AS SELECT val FROM t_recompress_projection SETTINGS optimize_use_projections = 0;
SELECT count() FROM t_recompress_projection WHERE val IN (SELECT val FROM expected_projection)
SETTINGS optimize_use_projections = 0;
SELECT count() FROM t_recompress_projection WHERE val IN (SELECT val FROM expected_projection)
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

CREATE TABLE t_recompress_materialized (key UInt64, val Float64 CODEC(ZSTD(1)))
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 100;
INSERT INTO t_recompress_materialized SELECT number, sin(number / 100.) * 100 FROM numbers(1000);
ALTER TABLE t_recompress_materialized MODIFY COLUMN val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01));
SYSTEM STOP MERGES t_recompress_materialized;
ALTER TABLE t_recompress_materialized RECOMPRESS COLUMN val SETTINGS mutations_sync = 0;
ALTER TABLE t_recompress_materialized ADD COLUMN m Float64 MATERIALIZED val * 2;
ALTER TABLE t_recompress_materialized MATERIALIZE COLUMN m SETTINGS mutations_sync = 0;
SYSTEM START MERGES t_recompress_materialized;
ALTER TABLE t_recompress_materialized MATERIALIZE COLUMN m SETTINGS mutations_sync = 2;

SELECT 'materialized';
SELECT count(), countIf(m != val * 2) FROM t_recompress_materialized;

SELECT 'mutations not done', count() FROM system.mutations
WHERE database = currentDatabase() AND table LIKE 't_recompress_%' AND NOT is_done;

DROP TABLE t_recompress_index;
DROP TABLE t_recompress_projection;
DROP TABLE t_recompress_materialized;
DROP TABLE expected_index;
DROP TABLE expected_projection;
