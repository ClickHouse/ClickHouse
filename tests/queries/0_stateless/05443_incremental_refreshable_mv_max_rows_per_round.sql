-- Tags: atomic-database
-- `stream_max_rows_per_round` and `stream_max_bytes_per_round` in the view's SETTINGS limit how much one refresh
-- of an incremental refreshable materialized view reads; the following refreshes continue from where it stopped.

SET async_insert = 0, max_insert_threads = 1, max_block_size = 65505, max_insert_block_size = 1048449;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS tgt_rows;
DROP TABLE IF EXISTS tgt_bytes;
DROP TABLE IF EXISTS mv_rows;
DROP TABLE IF EXISTS mv_bytes;

CREATE TABLE src (k UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

-- The limit is estimated from part sizes; single-block parts make it exact.
SYSTEM STOP MERGES src;

CREATE TABLE tgt_rows (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE tgt_bytes (k UInt64) ENGINE = MergeTree ORDER BY k;

CREATE MATERIALIZED VIEW mv_rows
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO tgt_rows EMPTY
    AS SELECT k FROM src SETTINGS stream_max_rows_per_round = 30;

-- A limit below one block still reads one block per refresh.
CREATE MATERIALIZED VIEW mv_bytes
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO tgt_bytes EMPTY
    AS SELECT k FROM src SETTINGS stream_max_bytes_per_round = 1;

-- Ten blocks of ten rows each.
INSERT INTO src SELECT number FROM numbers(0, 10);
INSERT INTO src SELECT number FROM numbers(10, 10);
INSERT INTO src SELECT number FROM numbers(20, 10);
INSERT INTO src SELECT number FROM numbers(30, 10);
INSERT INTO src SELECT number FROM numbers(40, 10);
INSERT INTO src SELECT number FROM numbers(50, 10);
INSERT INTO src SELECT number FROM numbers(60, 10);
INSERT INTO src SELECT number FROM numbers(70, 10);
INSERT INTO src SELECT number FROM numbers(80, 10);
INSERT INTO src SELECT number FROM numbers(90, 10);

SYSTEM REFRESH VIEW mv_rows;
SYSTEM WAIT VIEW mv_rows;
SELECT 'rows refresh 1', count() FROM tgt_rows;

SYSTEM REFRESH VIEW mv_rows;
SYSTEM WAIT VIEW mv_rows;
SELECT 'rows refresh 2', count() FROM tgt_rows;

SYSTEM REFRESH VIEW mv_rows;
SYSTEM WAIT VIEW mv_rows;
SELECT 'rows refresh 3', count() FROM tgt_rows;

SYSTEM REFRESH VIEW mv_rows;
SYSTEM WAIT VIEW mv_rows;
SELECT 'rows refresh 4', count() FROM tgt_rows;

-- Nothing is left to read, and every source row was appended exactly once.
SYSTEM REFRESH VIEW mv_rows;
SYSTEM WAIT VIEW mv_rows;
SELECT 'rows refresh 5', count(), uniqExact(k), sum(k) FROM tgt_rows;

SYSTEM REFRESH VIEW mv_bytes;
SYSTEM WAIT VIEW mv_bytes;
SELECT 'bytes refresh 1', count() FROM tgt_bytes;

SYSTEM REFRESH VIEW mv_bytes;
SYSTEM WAIT VIEW mv_bytes;
SELECT 'bytes refresh 2', count(), uniqExact(k) FROM tgt_bytes;

DROP TABLE mv_rows;
DROP TABLE mv_bytes;
DROP TABLE tgt_rows;
DROP TABLE tgt_bytes;
DROP TABLE src;
