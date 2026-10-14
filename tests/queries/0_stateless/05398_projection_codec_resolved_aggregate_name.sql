-- The old interpreter stores `countDistinct` under its resolved aggregate name.

DROP TABLE IF EXISTS t_projection_codec_count_distinct;

CREATE TABLE t_projection_codec_count_distinct
(
    k UInt8,
    v UInt64,
    PROJECTION p (c CODEC(ZSTD)) AS (SELECT k, countDistinct(v) AS c GROUP BY k)
)
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_projection_codec_count_distinct VALUES (1, 2), (1, 2), (1, 3), (2, 4);

SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_count_distinct' AND name = 'p'
    AND active AND column = 'uniqExact(v)';

SELECT k, countDistinct(v) FROM t_projection_codec_count_distinct GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, force_optimize_projection_name = 'p';

DETACH TABLE t_projection_codec_count_distinct;
ATTACH TABLE t_projection_codec_count_distinct;

SELECT k, countDistinct(v) FROM t_projection_codec_count_distinct GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, force_optimize_projection_name = 'p';

DROP TABLE t_projection_codec_count_distinct;
