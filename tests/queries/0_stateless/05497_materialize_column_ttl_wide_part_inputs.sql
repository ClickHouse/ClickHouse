-- `MATERIALIZE COLUMN` of a column that drives a column TTL runs the full TTL pass, which needs
-- all its inputs in the mutation block. On a wide part only the streamed columns are there.

-- An independent row TTL is executed by the full TTL pass too: it must delete the row from
-- every column, not only from the rewritten ones.
DROP TABLE IF EXISTS t_mat_ttl_row;
CREATE TABLE t_mat_ttl_row
(
    id UInt64,
    c DateTime MATERIALIZED toDateTime(2000000000),
    y UInt64 TTL c + INTERVAL 1 SECOND,
    d DateTime,
    INDEX ix (y, d) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 1 DAY
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP TTL MERGES t_mat_ttl_row;
INSERT INTO t_mat_ttl_row (id, y, d) VALUES (1, 100, toDateTime(1000000000)), (2, 200, toDateTime(2000000000));
ALTER TABLE t_mat_ttl_row MATERIALIZE COLUMN c SETTINGS mutations_sync = 2;
SELECT 'row ttl', id, toUnixTimestamp(c), y, toUnixTimestamp(d) FROM t_mat_ttl_row ORDER BY id;
DROP TABLE t_mat_ttl_row;

-- The same without a skip index: the row TTL input `d` must be fed by the mutation itself.
DROP TABLE IF EXISTS t_mat_ttl_row_noidx;
CREATE TABLE t_mat_ttl_row_noidx
(
    id UInt64,
    c DateTime MATERIALIZED toDateTime(2000000000),
    y UInt64 TTL c + INTERVAL 1 SECOND,
    d DateTime
)
ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 1 DAY
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP TTL MERGES t_mat_ttl_row_noidx;
INSERT INTO t_mat_ttl_row_noidx (id, y, d) VALUES (1, 100, toDateTime(1000000000)), (2, 200, toDateTime(2000000000));
ALTER TABLE t_mat_ttl_row_noidx MATERIALIZE COLUMN c SETTINGS mutations_sync = 2;
SELECT 'row ttl, no index', id, toUnixTimestamp(c), y, toUnixTimestamp(d) FROM t_mat_ttl_row_noidx ORDER BY id;
DROP TABLE t_mat_ttl_row_noidx;

-- A column TTL resets its target to the `DEFAULT` expression, whose inputs (`u`) differ from
-- the TTL expression inputs.
DROP TABLE IF EXISTS t_mat_ttl_default;
CREATE TABLE t_mat_ttl_default
(
    id UInt64,
    u UInt64,
    c DateTime MATERIALIZED toDateTime(2000000000),
    y UInt64 DEFAULT u + 1 TTL c + INTERVAL 1 SECOND
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP TTL MERGES t_mat_ttl_default;
INSERT INTO t_mat_ttl_default (id, u, y) VALUES (1, 7, 100);
ALTER TABLE t_mat_ttl_default MODIFY COLUMN c DateTime MATERIALIZED toDateTime(1000000000) SETTINGS mutations_sync = 2;
ALTER TABLE t_mat_ttl_default MATERIALIZE COLUMN c SETTINGS mutations_sync = 2;
SELECT 'default inputs', id, u, toUnixTimestamp(c), y FROM t_mat_ttl_default ORDER BY id;
DROP TABLE t_mat_ttl_default;

-- A recompression TTL reads the materialized column, and an independent column TTL reads `d`:
-- the TTL recalculation evaluates every TTL rule, so `d` must be in the block.
DROP TABLE IF EXISTS t_mat_ttl_recompress;
CREATE TABLE t_mat_ttl_recompress
(
    id UInt64,
    c DateTime MATERIALIZED toDateTime(2000000000),
    d DateTime,
    y UInt64 TTL d + INTERVAL 1 SECOND
)
ENGINE = MergeTree ORDER BY id
TTL c + INTERVAL 1 DAY RECOMPRESS CODEC(LZ4)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP TTL MERGES t_mat_ttl_recompress;
INSERT INTO t_mat_ttl_recompress (id, d, y) VALUES (1, toDateTime(2000000000), 100);
ALTER TABLE t_mat_ttl_recompress MODIFY COLUMN c DateTime MATERIALIZED toDateTime(2000000010) SETTINGS mutations_sync = 2;
ALTER TABLE t_mat_ttl_recompress MATERIALIZE COLUMN c SETTINGS mutations_sync = 2;
SELECT 'recompression', id, toUnixTimestamp(c), toUnixTimestamp(d), y FROM t_mat_ttl_recompress ORDER BY id;
DROP TABLE t_mat_ttl_recompress;
