-- `MATERIALIZE COLUMN` of a column read by a column TTL makes the mutation run the full TTL pass,
-- which executes every column TTL of the table, including independent ones.
-- See https://github.com/ClickHouse/ClickHouse/issues/93139

-- A projection reading the target of an independent column TTL must be rebuilt, on both part formats.
DROP TABLE IF EXISTS t_mat_indep_ttl_projection;
CREATE TABLE t_mat_indep_ttl_projection
(
    a UInt64,
    b String,
    c DateTime MATERIALIZED toDateTime(2000000000),
    y UInt64 TTL c + INTERVAL 1 SECOND,
    d DateTime,
    z UInt64 TTL d + INTERVAL 1 SECOND,
    PROJECTION p (SELECT a, z ORDER BY a)
)
ENGINE = MergeTree ORDER BY b
SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000;

SYSTEM STOP TTL MERGES t_mat_indep_ttl_projection;
INSERT INTO t_mat_indep_ttl_projection (a, b, y, d, z) VALUES (5, 'x', 7, toDateTime(1000000000), 123);
ALTER TABLE t_mat_indep_ttl_projection MATERIALIZE COLUMN c SETTINGS mutations_sync = 2;
SELECT 'compact', a, z FROM t_mat_indep_ttl_projection ORDER BY a SETTINGS optimize_use_projections = 0;
SELECT 'compact', a, z FROM t_mat_indep_ttl_projection ORDER BY a SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
DROP TABLE t_mat_indep_ttl_projection;

CREATE TABLE t_mat_indep_ttl_projection
(
    a UInt64,
    b String,
    c DateTime MATERIALIZED toDateTime(2000000000),
    y UInt64 TTL c + INTERVAL 1 SECOND,
    d DateTime,
    z UInt64 TTL d + INTERVAL 1 SECOND,
    PROJECTION p (SELECT a, z ORDER BY a)
)
ENGINE = MergeTree ORDER BY b
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP TTL MERGES t_mat_indep_ttl_projection;
INSERT INTO t_mat_indep_ttl_projection (a, b, y, d, z) VALUES (5, 'x', 7, toDateTime(1000000000), 123);
ALTER TABLE t_mat_indep_ttl_projection MATERIALIZE COLUMN c SETTINGS mutations_sync = 2;
SELECT 'wide', a, z FROM t_mat_indep_ttl_projection ORDER BY a SETTINGS optimize_use_projections = 0;
SELECT 'wide', a, z FROM t_mat_indep_ttl_projection ORDER BY a SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
DROP TABLE t_mat_indep_ttl_projection;

-- An independent column TTL on the is_deleted column would be executed by the same pass and could
-- flip the replacing semantics of existing rows, so the command is refused.
DROP TABLE IF EXISTS t_mat_indep_ttl_is_deleted;
CREATE TABLE t_mat_indep_ttl_is_deleted
(
    a UInt64,
    ver UInt64,
    c DateTime MATERIALIZED toDateTime(2000000000),
    y UInt64 TTL c + INTERVAL 1 SECOND,
    d DateTime,
    del UInt8 TTL d + INTERVAL 1 SECOND
)
ENGINE = ReplacingMergeTree(ver, del) ORDER BY a
SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000;

SYSTEM STOP TTL MERGES t_mat_indep_ttl_is_deleted;
INSERT INTO t_mat_indep_ttl_is_deleted (a, ver, y, d, del) VALUES (1, 1, 7, toDateTime(1000000000), 1);
ALTER TABLE t_mat_indep_ttl_is_deleted MATERIALIZE COLUMN c SETTINGS mutations_sync = 2; -- { serverError CANNOT_UPDATE_COLUMN }
SELECT count() FROM t_mat_indep_ttl_is_deleted FINAL;
DROP TABLE t_mat_indep_ttl_is_deleted;

-- A table TTL only deletes rows and never resets the version column, so it must not cause a refusal.
DROP TABLE IF EXISTS t_mat_rows_ttl_version;
CREATE TABLE t_mat_rows_ttl_version
(
    a UInt64,
    ver UInt64,
    c DateTime MATERIALIZED toDateTime(2000000000)
)
ENGINE = ReplacingMergeTree(ver) ORDER BY a TTL c + INTERVAL 1 DAY;

INSERT INTO t_mat_rows_ttl_version (a, ver) VALUES (1, 5);
ALTER TABLE t_mat_rows_ttl_version MATERIALIZE COLUMN c SETTINGS mutations_sync = 2;
SELECT a, ver FROM t_mat_rows_ttl_version FINAL;
DROP TABLE t_mat_rows_ttl_version;
