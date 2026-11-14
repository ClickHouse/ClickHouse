-- Tags: no-random-merge-tree-settings
-- MATERIALIZE COLUMN after changing the expression of a MATERIALIZED column must rebuild the
-- projections and skip indices that depend on it. https://github.com/ClickHouse/ClickHouse/issues/86742

SET mutations_sync = 2;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_materialize_column_wide;
DROP TABLE IF EXISTS t_materialize_column_compact;

CREATE TABLE t_materialize_column_wide
(
    id UInt32,
    d String,
    m UInt8 MATERIALIZED match(d, 'Python'),
    INDEX idx_m m TYPE minmax GRANULARITY 1,
    PROJECTION p (SELECT id, d, m ORDER BY d)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE t_materialize_column_compact
(
    id UInt32,
    d String,
    m UInt8 MATERIALIZED match(d, 'Python'),
    INDEX idx_m m TYPE minmax GRANULARITY 1,
    PROJECTION p (SELECT id, d, m ORDER BY d)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8, min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 0;

INSERT INTO t_materialize_column_wide (id, d) SELECT number, if(number % 10 = 0, 'python', 'other') FROM numbers(1000);
INSERT INTO t_materialize_column_compact (id, d) SELECT number, if(number % 10 = 0, 'python', 'other') FROM numbers(1000);

ALTER TABLE t_materialize_column_wide MODIFY COLUMN m UInt8 MATERIALIZED match(d, '(?i)Python');
ALTER TABLE t_materialize_column_wide MATERIALIZE COLUMN m;
ALTER TABLE t_materialize_column_compact MODIFY COLUMN m UInt8 MATERIALIZED match(d, '(?i)Python');
ALTER TABLE t_materialize_column_compact MATERIALIZE COLUMN m;

SELECT 'wide';
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_materialize_column_wide' AND active;
SELECT count() FROM t_materialize_column_wide WHERE d = 'python' AND m = 1 SETTINGS optimize_use_projections = 0, use_skip_indexes = 0;
SELECT count() FROM t_materialize_column_wide WHERE d = 'python' AND m = 1 SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'p', use_skip_indexes = 0;
SELECT count() FROM t_materialize_column_wide WHERE m = 1 SETTINGS optimize_use_projections = 0, use_skip_indexes = 0;
SELECT count() FROM t_materialize_column_wide WHERE m = 1 SETTINGS optimize_use_projections = 0, use_skip_indexes = 1, force_data_skipping_indices = 'idx_m';

SELECT 'compact';
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_materialize_column_compact' AND active;
SELECT count() FROM t_materialize_column_compact WHERE d = 'python' AND m = 1 SETTINGS optimize_use_projections = 0, use_skip_indexes = 0;
SELECT count() FROM t_materialize_column_compact WHERE d = 'python' AND m = 1 SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'p', use_skip_indexes = 0;
SELECT count() FROM t_materialize_column_compact WHERE m = 1 SETTINGS optimize_use_projections = 0, use_skip_indexes = 0;
SELECT count() FROM t_materialize_column_compact WHERE m = 1 SETTINGS optimize_use_projections = 0, use_skip_indexes = 1, force_data_skipping_indices = 'idx_m';

DROP TABLE t_materialize_column_wide;
DROP TABLE t_materialize_column_compact;
