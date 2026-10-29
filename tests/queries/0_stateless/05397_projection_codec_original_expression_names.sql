-- Tags: no-replicated-database
-- Tag no-replicated-database: plain `DETACH TABLE` is unsupported in a `Replicated` database.

-- Bind aliases to the names in the stored projection metadata, before Analyzer rewrites expressions.

SET distributed_ddl_entry_format_version = 5;
SET allow_projection_column_codecs_in_replicated_or_distributed_ddl = 1;

DROP TABLE IF EXISTS t_projection_codec_folded_name;
DROP TABLE IF EXISTS t_projection_codec_interval_name;
DROP TABLE IF EXISTS t_projection_codec_nulls_name;

CREATE TABLE t_projection_codec_folded_name
(
    k UInt64,
    v UInt64,
    PROJECTION p (s CODEC(ZSTD)) AS (SELECT k, sum(v + toUInt8(1)) AS s GROUP BY k)
)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_projection_codec_folded_name VALUES (1, 2), (1, 3), (2, 4);
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_folded_name' AND name = 'p'
    AND active AND column = 'sum(plus(v, toUInt8(1)))';
SELECT position(create_table_query, '`s` CODEC(ZSTD)') > 0 FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_codec_folded_name';
SELECT k, sum(v + toUInt8(1)) FROM t_projection_codec_folded_name GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 0;
DETACH TABLE t_projection_codec_folded_name;
ATTACH TABLE t_projection_codec_folded_name;
SELECT k, sum(v + toUInt8(1)) FROM t_projection_codec_folded_name GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 0;

CREATE TABLE t_projection_codec_interval_name
(
    ts DateTime('UTC'),
    v UInt64,
    PROJECTION p (bucket CODEC(ZSTD)) AS
        (SELECT toStartOfInterval(ts, INTERVAL 5 MINUTE) AS bucket, sum(v) AS s GROUP BY bucket)
)
ENGINE = MergeTree ORDER BY ts;
INSERT INTO t_projection_codec_interval_name VALUES ('2026-10-14 12:01:00', 2), ('2026-10-14 12:02:00', 3);
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_interval_name' AND name = 'p'
    AND active AND column = 'toStartOfInterval(ts, toIntervalMinute(5))';
SELECT toUnixTimestamp(toStartOfInterval(ts, INTERVAL 5 MINUTE)), sum(v)
FROM t_projection_codec_interval_name GROUP BY 1
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, force_optimize_projection_name = 'p';

CREATE TABLE t_projection_codec_nulls_name
(
    k UInt64,
    v Nullable(UInt64),
    PROJECTION p (c CODEC(ZSTD)) AS (SELECT k, count(v) IGNORE NULLS AS c GROUP BY k)
)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_projection_codec_nulls_name VALUES (1, 2), (1, NULL);
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_nulls_name' AND name = 'p'
    AND active AND column = 'count(v) IGNORE NULLS';
SELECT k, count(v) IGNORE NULLS FROM t_projection_codec_nulls_name GROUP BY k;

DROP TABLE t_projection_codec_nulls_name;
DROP TABLE t_projection_codec_interval_name;
DROP TABLE t_projection_codec_folded_name;
