-- A column fully expired by TTL is not dropped from the part without being read when something is
-- computed from it: that has to be recomputed from the default the column takes.

-- A `MATERIALIZED` column, merging a single part.
DROP TABLE IF EXISTS t_ttl_dependents_single;
CREATE TABLE t_ttl_dependents_single
(
    d DateTime,
    x Int32 TTL d + INTERVAL 1 SECOND,
    y Int32,
    mx Int32 MATERIALIZED x + 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_ttl_dependents_single (d, x, y) VALUES ('2000-01-01 00:00:00', 41, 5);
OPTIMIZE TABLE t_ttl_dependents_single FINAL;
SELECT x, mx, y FROM t_ttl_dependents_single;

DROP TABLE t_ttl_dependents_single;

-- A `MATERIALIZED` column, merging several parts.
DROP TABLE IF EXISTS t_ttl_dependents_multiple;
CREATE TABLE t_ttl_dependents_multiple
(
    d DateTime,
    x Int32 TTL d + INTERVAL 1 SECOND,
    y Int32,
    mx Int32 MATERIALIZED x + 1
)
ENGINE = MergeTree ORDER BY y SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_ttl_dependents_multiple (d, x, y) VALUES ('2000-01-01 00:00:00', 41, 5);
INSERT INTO t_ttl_dependents_multiple (d, x, y) VALUES ('2000-01-02 00:00:00', 42, 6);
OPTIMIZE TABLE t_ttl_dependents_multiple FINAL;
SELECT x, mx, y FROM t_ttl_dependents_multiple ORDER BY y;

DROP TABLE t_ttl_dependents_multiple;

-- The TTL info of a `RECOMPRESS` TTL that reads the column.
DROP TABLE IF EXISTS t_ttl_dependents_recompress;
CREATE TABLE t_ttl_dependents_recompress
(
    d DateTime,
    x Int32 TTL d + INTERVAL 1 SECOND,
    y Int32
)
ENGINE = MergeTree ORDER BY tuple()
TTL if(x = 0, toDateTime('2000-01-01 00:00:00', 'UTC'), toDateTime('2100-01-01 00:00:00', 'UTC')) RECOMPRESS CODEC(ZSTD(1))
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_ttl_dependents_recompress (d, x, y) VALUES ('2000-01-01 00:00:00', 41, 5);
OPTIMIZE TABLE t_ttl_dependents_recompress FINAL;
SELECT x, y FROM t_ttl_dependents_recompress;
SELECT toTimeZone(recompression_ttl_info.max[1], 'UTC') FROM system.parts
WHERE database = currentDatabase() AND table = 't_ttl_dependents_recompress' AND active;

DROP TABLE t_ttl_dependents_recompress;
