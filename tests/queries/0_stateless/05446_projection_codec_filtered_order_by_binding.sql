-- Tags: no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings
-- Tag no-replicated-database: DDL there is published to Keeper, which projection column codecs gate behind a setting.

-- A filtered normal projection stores its `ORDER BY` as the columns that expression reads, appended after the
-- `SELECT` outputs. A codec declaration must still bind to the `SELECT` outputs only: a column read only by
-- the `ORDER BY` is not declarable, and a selected column that the `ORDER BY` also reads is not ambiguous.

DROP TABLE IF EXISTS t_filtered_codec;

-- `k` is selected once and also read by the `ORDER BY`; `CODEC(NONE)` makes its use visible in the part sizes.
CREATE TABLE t_filtered_codec
(
    k UInt64,
    j UInt64,
    v UInt64,
    PROJECTION p (k CODEC(NONE)) AS (SELECT k, v WHERE v > 0 ORDER BY (k, j))
)
ENGINE = MergeTree ORDER BY v SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_filtered_codec SELECT number, number, number + 1 FROM numbers(100000);

SELECT 'create', column, column_data_compressed_bytes > column_data_uncompressed_bytes AS is_uncompressed
FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_filtered_codec' AND active AND name = 'p' AND column IN ('k', 'v')
ORDER BY column;

DROP TABLE t_filtered_codec;

-- Columns read only by the `ORDER BY` are not outputs of the projection.
CREATE TABLE t_filtered_codec (k UInt64, j UInt64, v UInt64, PROJECTION p (k CODEC(NONE)) AS (SELECT v WHERE v > 0 ORDER BY (k, j)))
ENGINE = MergeTree ORDER BY v; -- { serverError THERE_IS_NO_COLUMN }

CREATE TABLE t_filtered_codec (k UInt64, j UInt64, v UInt64, PROJECTION p (j CODEC(NONE)) AS (SELECT k, v WHERE v > 0 ORDER BY (k, j)))
ENGINE = MergeTree ORDER BY v; -- { serverError THERE_IS_NO_COLUMN }

-- The same through `ALTER TABLE ... ADD PROJECTION`.
CREATE TABLE t_filtered_codec (k UInt64, j UInt64, v UInt64)
ENGINE = MergeTree ORDER BY v SETTINGS min_bytes_for_wide_part = 0;

ALTER TABLE t_filtered_codec ADD PROJECTION p_ordering_only (k CODEC(NONE)) AS (SELECT v WHERE v > 0 ORDER BY (k, j)); -- { serverError THERE_IS_NO_COLUMN }
ALTER TABLE t_filtered_codec ADD PROJECTION p (k CODEC(NONE)) AS (SELECT k, v WHERE v > 0 ORDER BY (k, j));

INSERT INTO t_filtered_codec SELECT number, number, number + 1 FROM numbers(100000);

SELECT 'alter', column, column_data_compressed_bytes > column_data_uncompressed_bytes AS is_uncompressed
FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_filtered_codec' AND active AND name = 'p' AND column IN ('k', 'v')
ORDER BY column;

DROP TABLE t_filtered_codec;
