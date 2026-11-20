-- Tags: no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_projection_codec_aggregate_rewrite_create;
DROP TABLE IF EXISTS t_projection_codec_aggregate_rewrite_alter;

-- The resolved spelling of `a` must not make the distinct stored output `b` ambiguous.
CREATE TABLE t_projection_codec_aggregate_rewrite_create
(
    k UInt8,
    v Nullable(UInt64),
    PROJECTION p (a CODEC(ZSTD)) AS
        (SELECT k, count(v) IGNORE NULLS AS a, count(v) AS b GROUP BY k)
)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_projection_codec_aggregate_rewrite_create VALUES (1, 2), (1, NULL), (2, 3);
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_aggregate_rewrite_create'
    AND name = 'p' AND active AND column = 'count(v) IGNORE NULLS';
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_aggregate_rewrite_create'
    AND name = 'p' AND active AND column = 'count(v)';
SELECT position(create_table_query, '`a` CODEC(ZSTD)') > 0 FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_codec_aggregate_rewrite_create';
DETACH TABLE t_projection_codec_aggregate_rewrite_create;
ATTACH TABLE t_projection_codec_aggregate_rewrite_create;
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_aggregate_rewrite_create'
    AND name = 'p' AND active AND column = 'count(v) IGNORE NULLS';

-- The same binding must hold when `ALTER` adds and materializes a projection.
CREATE TABLE t_projection_codec_aggregate_rewrite_alter (k UInt8, v Nullable(UInt64))
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_projection_codec_aggregate_rewrite_alter VALUES (1, 2), (1, NULL), (2, 3);
ALTER TABLE t_projection_codec_aggregate_rewrite_alter ADD PROJECTION p
    (a CODEC(ZSTD)) AS (SELECT k, count(v) IGNORE NULLS AS a, count(v) AS b GROUP BY k);
ALTER TABLE t_projection_codec_aggregate_rewrite_alter MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_aggregate_rewrite_alter'
    AND name = 'p' AND active AND column = 'count(v) IGNORE NULLS';
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_aggregate_rewrite_alter'
    AND name = 'p' AND active AND column = 'count(v)';
DETACH TABLE t_projection_codec_aggregate_rewrite_alter;
ATTACH TABLE t_projection_codec_aggregate_rewrite_alter;
SELECT position(create_table_query, '`a` CODEC(ZSTD)') > 0 FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_codec_aggregate_rewrite_alter';

DROP TABLE t_projection_codec_aggregate_rewrite_alter;
DROP TABLE t_projection_codec_aggregate_rewrite_create;
