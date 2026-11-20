-- Tags: no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_projection_codec_untuple_create;
DROP TABLE IF EXISTS t_projection_codec_untuple_alter;

-- One `untuple` expression yields two outputs between the declared codec targets.
CREATE TABLE t_projection_codec_untuple_create
(
    k UInt64,
    v UInt64,
    j UInt64,
    w UInt64,
    PROJECTION p (k CODEC(ZSTD), tail CODEC(ZSTD)) AS
        (SELECT k, untuple(tuple(v, j)), w AS tail ORDER BY k)
)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_projection_codec_untuple_create VALUES (1, 2, 3, 7), (2, 4, 5, 11);
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_untuple_create'
    AND name = 'p' AND active AND column = 'k';
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_untuple_create'
    AND name = 'p' AND active AND column = 'w';
SELECT k, w FROM t_projection_codec_untuple_create ORDER BY k;
DETACH TABLE t_projection_codec_untuple_create;
ATTACH TABLE t_projection_codec_untuple_create;
SELECT position(create_table_query, '`tail` CODEC(ZSTD)') > 0 FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_codec_untuple_create';

-- `ALTER` and materialization must use the same expanded output positions.
CREATE TABLE t_projection_codec_untuple_alter (k UInt64, v UInt64, j UInt64)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_projection_codec_untuple_alter VALUES (1, 2, 3), (2, 4, 5);
ALTER TABLE t_projection_codec_untuple_alter ADD PROJECTION p
    (k CODEC(ZSTD)) AS (SELECT k, untuple(tuple(v, j)) ORDER BY k);
ALTER TABLE t_projection_codec_untuple_alter MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_untuple_alter'
    AND name = 'p' AND active AND column = 'k';
SELECT k, v, j FROM t_projection_codec_untuple_alter ORDER BY k;
DETACH TABLE t_projection_codec_untuple_alter;
ATTACH TABLE t_projection_codec_untuple_alter;
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_untuple_alter'
    AND name = 'p' AND active AND column = 'k';

DROP TABLE t_projection_codec_untuple_alter;
DROP TABLE t_projection_codec_untuple_create;
