-- Both `count()` spellings are stored, but the codec only targets `k`.

SET distributed_ddl_entry_format_version = 5;
SET allow_projection_column_codecs_in_replicated_or_distributed_ddl = 1;

DROP TABLE IF EXISTS t_projection_codec_unbound_aggregates;

CREATE TABLE t_projection_codec_unbound_aggregates
(
    k UInt8,
    v Nullable(UInt64),
    PROJECTION p (k CODEC(ZSTD)) AS
        (SELECT k, count() IGNORE NULLS AS a, count() AS b GROUP BY k)
)
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_projection_codec_unbound_aggregates VALUES (1, 2), (1, NULL);

SELECT column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_unbound_aggregates'
    AND name = 'p' AND active ORDER BY column;

DROP TABLE t_projection_codec_unbound_aggregates;
