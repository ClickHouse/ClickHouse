-- Tags: no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_projection_codec_alias_aggregate_binding;
DROP TABLE IF EXISTS t_projection_codec_alias_filtered_binding;
DROP TABLE IF EXISTS t_projection_codec_star_alias_binding;
DROP TABLE IF EXISTS t_projection_codec_star_alias_bad;
DROP TABLE IF EXISTS t_projection_codec_order_match_binding;
DROP TABLE IF EXISTS t_projection_codec_hidden_order_binding;

-- A table `ALIAS` is expanded before the aggregate column is stored.
CREATE TABLE t_projection_codec_alias_aggregate_binding
(
    k UInt64,
    v UInt64,
    av UInt64 ALIAS v + toUInt8(1),
    PROJECTION p (s CODEC(ZSTD)) AS (SELECT k, sum(av) AS s GROUP BY k)
)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_projection_codec_alias_aggregate_binding VALUES (1, 2), (1, 3), (2, 4);
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_alias_aggregate_binding'
    AND name = 'p' AND active AND column = 'sum(plus(v, toUInt8(1)))';
SELECT k, sum(av) FROM t_projection_codec_alias_aggregate_binding GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, force_optimize_projection_name = 'p';
DETACH TABLE t_projection_codec_alias_aggregate_binding;
ATTACH TABLE t_projection_codec_alias_aggregate_binding;

-- A filtered normal projection uses the same expanded physical expression.
CREATE TABLE t_projection_codec_alias_filtered_binding
(
    k UInt64,
    v UInt64,
    av UInt64 ALIAS v + toUInt8(1),
    PROJECTION p (inc CODEC(ZSTD)) AS (SELECT k, av AS inc WHERE k > 0 ORDER BY k)
)
ENGINE = MergeTree ORDER BY v;
INSERT INTO t_projection_codec_alias_filtered_binding VALUES (1, 2), (2, 3), (0, 4);
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_alias_filtered_binding'
    AND name = 'p' AND active AND column = 'plus(v, toUInt8(1))';
DETACH TABLE t_projection_codec_alias_filtered_binding;
ATTACH TABLE t_projection_codec_alias_filtered_binding;

-- `SELECT *` omits table `ALIAS` columns by default, but still exposes ordinary columns.
CREATE TABLE t_projection_codec_star_alias_binding
(
    k UInt64,
    v UInt64,
    av UInt64 ALIAS v + toUInt8(1),
    PROJECTION p (v CODEC(ZSTD)) AS (SELECT * ORDER BY k)
)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_projection_codec_star_alias_binding VALUES (1, 2);
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_star_alias_binding'
    AND name = 'p' AND active AND column = 'v';
DETACH TABLE t_projection_codec_star_alias_binding;
ATTACH TABLE t_projection_codec_star_alias_binding;

CREATE TABLE t_projection_codec_star_alias_bad
(
    k UInt64,
    v UInt64,
    av UInt64 ALIAS v + toUInt8(1),
    PROJECTION p (av CODEC(ZSTD)) AS (SELECT * ORDER BY k)
)
ENGINE = MergeTree ORDER BY k; -- { serverError THERE_IS_NO_COLUMN }
SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_codec_star_alias_bad';

-- Expanding `ORDER BY` to multiple columns must not hide a visible `SELECT` output.
CREATE TABLE t_projection_codec_order_match_binding
(
    v UInt64,
    k UInt64,
    PROJECTION p (v CODEC(ZSTD)) AS (SELECT v ORDER BY COLUMNS('^(v|k)$'))
)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_projection_codec_order_match_binding VALUES (2, 1), (1, 2);
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_order_match_binding'
    AND name = 'p' AND active AND column = 'v';

-- Neither of the extra `ORDER BY` columns is an output of the original `SELECT`.
CREATE TABLE t_projection_codec_hidden_order_binding
(
    k UInt64,
    j UInt64,
    v UInt64,
    PROJECTION p (k CODEC(ZSTD)) AS (SELECT v ORDER BY COLUMNS('^(k|j)$'))
)
ENGINE = MergeTree ORDER BY v; -- { serverError THERE_IS_NO_COLUMN }
CREATE TABLE t_projection_codec_hidden_order_binding
(
    k UInt64,
    j UInt64,
    v UInt64,
    PROJECTION p (j CODEC(ZSTD)) AS (SELECT v ORDER BY COLUMNS('^(k|j)$'))
)
ENGINE = MergeTree ORDER BY v; -- { serverError THERE_IS_NO_COLUMN }
SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_codec_hidden_order_binding';

DROP TABLE t_projection_codec_order_match_binding;
DROP TABLE t_projection_codec_star_alias_binding;
DROP TABLE t_projection_codec_alias_filtered_binding;
DROP TABLE t_projection_codec_alias_aggregate_binding;
