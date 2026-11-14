-- A `Buffer` keeps the pending `Too many parts` decision of every query whose rows it holds in memory, and a
-- flush writing those rows runs the pending checks with the limits of these queries. If the destination table is
-- detached and attached again in between, the flush writes into the new instance of the table, so the pending
-- check must run against it rather than be skipped because the instance it was created for is gone.

DROP TABLE IF EXISTS t_05621_mv_direct;
DROP TABLE IF EXISTS t_05621_mv_buf;
DROP TABLE IF EXISTS t_05621_mv_hidden;
DROP TABLE IF EXISTS t_05621_src;
DROP TABLE IF EXISTS t_05621_buf;
DROP TABLE IF EXISTS t_05621_hidden;
DROP TABLE IF EXISTS t_05621_dst;

CREATE TABLE t_05621_dst (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 1;
SYSTEM STOP MERGES t_05621_dst;

CREATE TABLE t_05621_hidden (n UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW t_05621_mv_hidden TO t_05621_dst AS SELECT n FROM t_05621_hidden;

-- Small blocks stay in memory.
CREATE TABLE t_05621_buf (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05621_hidden, 1, 1000000, 1000000, 1000000, 1000000, 1000000000, 1000000000);

CREATE TABLE t_05621_src (n UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW t_05621_mv_direct TO t_05621_dst AS SELECT n FROM t_05621_src;
CREATE MATERIALIZED VIEW t_05621_mv_buf TO t_05621_buf AS SELECT n + 1000 AS n FROM t_05621_src;

-- The first query is accepted while the destination has no parts: the direct branch writes a part, the buffer
-- keeps the row.
INSERT INTO t_05621_src SETTINGS max_insert_threads = 1, parallel_view_processing = 0 VALUES (1);
SELECT n FROM t_05621_dst ORDER BY n;

-- The second query only adds a row to the buffer and finishes; its decision is still pending.
INSERT INTO t_05621_buf VALUES (2000);

-- Replace the instance of the destination table.
DETACH TABLE t_05621_dst SYNC;
ATTACH TABLE t_05621_dst;
SYSTEM STOP MERGES t_05621_dst;

-- The flush writes the rows of both queries. The pending check of the second query sees the part written by the
-- first one and rejects the flush.
OPTIMIZE TABLE t_05621_buf; -- { serverError TOO_MANY_PARTS }
SELECT count() FROM t_05621_dst;

DROP TABLE t_05621_mv_direct;
DROP TABLE t_05621_mv_buf;
DROP TABLE t_05621_src;
DROP TABLE t_05621_buf;
DROP TABLE t_05621_mv_hidden;
DROP TABLE t_05621_hidden;
DROP TABLE t_05621_dst;
