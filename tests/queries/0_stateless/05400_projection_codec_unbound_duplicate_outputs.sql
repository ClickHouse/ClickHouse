-- Tags: no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_projection_codec_duplicate_aggregate;
DROP TABLE IF EXISTS t_projection_codec_duplicate_normal;

-- Both aliases share one stored aggregate column; the codec targets only the key.
CREATE TABLE t_projection_codec_duplicate_aggregate
(
    k UInt64,
    v UInt64,
    PROJECTION p (k CODEC(NONE)) AS (SELECT k, sum(v) AS a, sum(v) AS b GROUP BY k)
)
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_projection_codec_duplicate_aggregate VALUES (1, 2), (1, 3), (2, 4);
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_duplicate_aggregate'
    AND name = 'p' AND active AND column = 'sum(v)';
SELECT k, sum(v) FROM t_projection_codec_duplicate_aggregate GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, force_optimize_projection_name = 'p';

-- The same binding rule applies when ordinary outputs share a stored column.
CREATE TABLE t_projection_codec_duplicate_normal
(
    k UInt64,
    v UInt64,
    PROJECTION p (k CODEC(NONE)) AS (SELECT k, v AS a, v AS b ORDER BY v)
)
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_projection_codec_duplicate_normal VALUES (1, 4), (2, 1);
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_duplicate_normal'
    AND name = 'p' AND active AND column = 'v';
SELECT k, v FROM t_projection_codec_duplicate_normal ORDER BY v
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, force_optimize_projection_name = 'p';

DROP TABLE t_projection_codec_duplicate_normal;
DROP TABLE t_projection_codec_duplicate_aggregate;
