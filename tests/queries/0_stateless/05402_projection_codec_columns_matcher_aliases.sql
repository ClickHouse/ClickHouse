-- Tags: no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_projection_codec_matcher_alias;
DROP TABLE IF EXISTS t_projection_codec_matcher_materialized;
DROP TABLE IF EXISTS t_projection_codec_matcher_original_name;

-- A `COLUMNS` matcher uses the projection writer's ordinary-column expansion.
CREATE TABLE t_projection_codec_matcher_alias
(
    k UInt64,
    v UInt64,
    av UInt64 ALIAS v + 1,
    w UInt64,
    PROJECTION p (w CODEC(ZSTD)) AS (SELECT COLUMNS('.*') ORDER BY k)
)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_projection_codec_matcher_alias (k, v, w) VALUES (1, 2, 3), (2, 4, 5);
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_matcher_alias'
    AND name = 'p' AND active AND column = 'w';
SELECT k, w FROM t_projection_codec_matcher_alias ORDER BY k;
DETACH TABLE t_projection_codec_matcher_alias;
ATTACH TABLE t_projection_codec_matcher_alias;

-- The same expansion must be used when `ALTER` adds a projection after data was written.
CREATE TABLE t_projection_codec_matcher_materialized
(
    k UInt64,
    v UInt64,
    mv UInt64 MATERIALIZED v + 2,
    w UInt64
)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_projection_codec_matcher_materialized (k, v, w) VALUES (1, 2, 3), (2, 4, 5);
ALTER TABLE t_projection_codec_matcher_materialized ADD PROJECTION p
    (w CODEC(ZSTD)) AS (SELECT COLUMNS('.*') ORDER BY k);
ALTER TABLE t_projection_codec_matcher_materialized MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_matcher_materialized'
    AND name = 'p' AND active AND column = 'w';
SELECT k, w FROM t_projection_codec_matcher_materialized ORDER BY k;
DETACH TABLE t_projection_codec_matcher_materialized;
ATTACH TABLE t_projection_codec_matcher_materialized;

-- Matcher expansion must preserve original names of other `SELECT` expressions.
CREATE TABLE t_projection_codec_matcher_original_name
(
    k UInt64,
    v UInt64,
    PROJECTION p (`countDistinct(v)` CODEC(ZSTD)) AS (SELECT COLUMNS('^k$'), countDistinct(v) GROUP BY k)
)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_projection_codec_matcher_original_name VALUES (1, 2), (1, 3);
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_matcher_original_name'
    AND name = 'p' AND active AND column = 'uniqExact(v)';

DROP TABLE t_projection_codec_matcher_original_name;
DROP TABLE t_projection_codec_matcher_materialized;
DROP TABLE t_projection_codec_matcher_alias;
