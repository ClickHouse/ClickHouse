-- `MATERIALIZE COLUMN` of a column that drives a column TTL runs the full TTL pass.

-- A column TTL on the parent of an engine subcolumn (`flags.del` is the is_deleted column) can
-- reset the merge-semantic value, so the command is refused.
DROP TABLE IF EXISTS t_mat_ttl_subcolumn_key;
CREATE TABLE t_mat_ttl_subcolumn_key
(
    a UInt64,
    ver UInt64,
    c DateTime MATERIALIZED toDateTime(2000000000),
    y UInt64 TTL c + INTERVAL 1 SECOND,
    d DateTime,
    flags Tuple(del UInt8, other UInt64) TTL d + INTERVAL 1 SECOND
)
ENGINE = ReplacingMergeTree(ver, `flags.del`) ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP TTL MERGES t_mat_ttl_subcolumn_key;
INSERT INTO t_mat_ttl_subcolumn_key (a, ver, y, d, flags) VALUES (1, 1, 7, toDateTime(1000000000), (1, 9));
SELECT 'subcolumn key before', count() FROM t_mat_ttl_subcolumn_key FINAL;
ALTER TABLE t_mat_ttl_subcolumn_key MATERIALIZE COLUMN c SETTINGS mutations_sync = 2; -- { serverError CANNOT_UPDATE_COLUMN }
SELECT 'subcolumn key after', flags FROM t_mat_ttl_subcolumn_key;
SELECT 'subcolumn key after', count() FROM t_mat_ttl_subcolumn_key FINAL;
DROP TABLE t_mat_ttl_subcolumn_key;

-- An independent recompression TTL is executed by the full TTL pass too, so its input `d` must be
-- fed on a wide part.
DROP TABLE IF EXISTS t_mat_ttl_recompress;
CREATE TABLE t_mat_ttl_recompress
(
    id UInt64,
    c DateTime MATERIALIZED toDateTime(2000000000),
    y UInt64 TTL c + INTERVAL 1 SECOND,
    d DateTime
)
ENGINE = MergeTree ORDER BY id
TTL d + INTERVAL 1 DAY RECOMPRESS CODEC(LZ4)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP TTL MERGES t_mat_ttl_recompress;
INSERT INTO t_mat_ttl_recompress (id, y, d) VALUES (1, 100, toDateTime(2000000000));
ALTER TABLE t_mat_ttl_recompress MATERIALIZE COLUMN c SETTINGS mutations_sync = 2;
SELECT 'recompress', id, toUnixTimestamp(c), y, toUnixTimestamp(d) FROM t_mat_ttl_recompress;
DROP TABLE t_mat_ttl_recompress;

-- A TTL target whose `DEFAULT` reads an `ALIAS` column is reset to the expanded alias definition.
DROP TABLE IF EXISTS t_mat_ttl_default_alias;
CREATE TABLE t_mat_ttl_default_alias
(
    id UInt64,
    u UInt64,
    a UInt64 ALIAS u + 10,
    c DateTime MATERIALIZED toDateTime(2000000000),
    y UInt64 DEFAULT a + 1 TTL c + INTERVAL 1 SECOND
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP TTL MERGES t_mat_ttl_default_alias;
INSERT INTO t_mat_ttl_default_alias (id, u, y) VALUES (1, 7, 100);
ALTER TABLE t_mat_ttl_default_alias MODIFY COLUMN c DateTime MATERIALIZED toDateTime(1000000000);
ALTER TABLE t_mat_ttl_default_alias MATERIALIZE COLUMN c SETTINGS mutations_sync = 2;
SELECT 'default alias wide', id, u, toUnixTimestamp(c), y FROM t_mat_ttl_default_alias;
DROP TABLE t_mat_ttl_default_alias;

-- The same on a compact part, where the block already holds `u`.
DROP TABLE IF EXISTS t_mat_ttl_default_alias_compact;
CREATE TABLE t_mat_ttl_default_alias_compact
(
    id UInt64,
    u UInt64,
    a UInt64 ALIAS u + 10,
    c DateTime MATERIALIZED toDateTime(2000000000),
    y UInt64 DEFAULT a + 1 TTL c + INTERVAL 1 SECOND
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

SYSTEM STOP TTL MERGES t_mat_ttl_default_alias_compact;
INSERT INTO t_mat_ttl_default_alias_compact (id, u, y) VALUES (1, 7, 100);
ALTER TABLE t_mat_ttl_default_alias_compact MODIFY COLUMN c DateTime MATERIALIZED toDateTime(1000000000);
ALTER TABLE t_mat_ttl_default_alias_compact MATERIALIZE COLUMN c SETTINGS mutations_sync = 2;
SELECT 'default alias compact', id, u, toUnixTimestamp(c), y FROM t_mat_ttl_default_alias_compact;
DROP TABLE t_mat_ttl_default_alias_compact;
