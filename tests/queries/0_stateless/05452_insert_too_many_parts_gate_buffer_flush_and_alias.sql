-- The limits of a `Buffer`'s destination are enforced when the buffer actually writes into it: on a flush
-- by threshold as well as on a direct write, even if a sibling branch of the query writing into the
-- destination directly has already made the `Too many parts` decision with looser limits. A `Buffer`
-- behind a forwarding storage (an `Alias`) that only keeps the rows in memory is not subject to them.

DROP TABLE IF EXISTS t_05452_mv_direct;
DROP TABLE IF EXISTS t_05452_mv_buf;
DROP TABLE IF EXISTS t_05452_src;
DROP TABLE IF EXISTS t_05452_alias;
DROP TABLE IF EXISTS t_05452_buf_flush;
DROP TABLE IF EXISTS t_05452_buf_memory;
DROP TABLE IF EXISTS t_05452_dst;

CREATE TABLE t_05452_dst (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 1;

-- One row stays in memory, the next block flushes it by threshold.
CREATE TABLE t_05452_buf_flush (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05452_dst, 1, 1000000, 1000000, 1000000, 1, 1000000000, 1000000000);

-- Small blocks stay in memory.
CREATE TABLE t_05452_buf_memory (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05452_dst, 1, 1000000, 1000000, 1000000, 1000000, 1000000000, 1000000000);

CREATE TABLE t_05452_alias ENGINE = Alias('t_05452_buf_memory');

CREATE TABLE t_05452_src (n UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW t_05452_mv_direct TO t_05452_dst AS SELECT n FROM t_05452_src;
CREATE MATERIALIZED VIEW t_05452_mv_buf TO t_05452_buf_flush AS SELECT n + 1000 AS n FROM t_05452_src;

INSERT INTO t_05452_dst VALUES (100);

-- The query raises the limit for the direct branch, but the flush of the buffer runs with the limit of
-- the table, and the table already has one part.
INSERT INTO t_05452_src SELECT number FROM numbers(2)
    SETTINGS parts_to_throw_insert = 1000, max_insert_threads = 1, parallel_view_processing = 0,
    max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0; -- { serverError TOO_MANY_PARTS }

-- Nothing has been flushed through the buffer.
SELECT count() FROM t_05452_dst WHERE n >= 1000;

-- The buffer behind the alias keeps the row in memory, as a direct insert into the buffer does.
INSERT INTO t_05452_buf_memory VALUES (1);
INSERT INTO t_05452_alias VALUES (2);
SELECT n FROM t_05452_buf_memory WHERE n < 100 ORDER BY n;

DROP TABLE t_05452_mv_direct;
DROP TABLE t_05452_mv_buf;
DROP TABLE t_05452_src;
DROP TABLE t_05452_alias;
DROP TABLE t_05452_buf_flush;
DROP TABLE t_05452_buf_memory;
DROP TABLE t_05452_dst;
