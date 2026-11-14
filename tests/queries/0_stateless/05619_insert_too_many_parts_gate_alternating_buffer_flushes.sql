-- Two memory-only `Buffer` tables over one destination, fed by every query through materialized views, and flushed
-- alternately while each still holds the row of the latest query. Each flush writes on behalf of the queries whose
-- rows it holds and borrows the pre-write decision a participating query has already made for the destination -
-- possibly a decision that an earlier flush has borrowed itself. The borrowed decision is always that of the gate
-- which has made it, rather than a growing chain of the gates of all the earlier flushes.

DROP TABLE IF EXISTS t_05619_mv_direct;
DROP TABLE IF EXISTS t_05619_mv_x;
DROP TABLE IF EXISTS t_05619_mv_y;
DROP TABLE IF EXISTS t_05619_src;
DROP TABLE IF EXISTS t_05619_buf_x;
DROP TABLE IF EXISTS t_05619_buf_y;
DROP TABLE IF EXISTS t_05619_dst;

CREATE TABLE t_05619_dst (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS parts_to_throw_insert = 1000;
CREATE TABLE t_05619_buf_x (n UInt64) ENGINE = Buffer(currentDatabase(), t_05619_dst, 1, 1000000, 1000000, 1000000000, 1000000000, 1000000000000, 1000000000000);
CREATE TABLE t_05619_buf_y (n UInt64) ENGINE = Buffer(currentDatabase(), t_05619_dst, 1, 1000000, 1000000, 1000000000, 1000000000, 1000000000000, 1000000000000);
CREATE TABLE t_05619_src (n UInt64) ENGINE = Null;

-- The first query writes into the destination directly as well, so it makes a decision a flush can borrow.
CREATE MATERIALIZED VIEW t_05619_mv_direct TO t_05619_dst AS SELECT n FROM t_05619_src WHERE n = 0;
CREATE MATERIALIZED VIEW t_05619_mv_x TO t_05619_buf_x AS SELECT n * 10 + 1 AS n FROM t_05619_src;
CREATE MATERIALIZED VIEW t_05619_mv_y TO t_05619_buf_y AS SELECT n * 10 + 2 AS n FROM t_05619_src;

INSERT INTO t_05619_src VALUES (0);
INSERT INTO t_05619_src VALUES (1);
OPTIMIZE TABLE t_05619_buf_x;
INSERT INTO t_05619_src VALUES (2);
OPTIMIZE TABLE t_05619_buf_y;
INSERT INTO t_05619_src VALUES (3);
OPTIMIZE TABLE t_05619_buf_x;
INSERT INTO t_05619_src VALUES (4);
OPTIMIZE TABLE t_05619_buf_y;
INSERT INTO t_05619_src VALUES (5);
OPTIMIZE TABLE t_05619_buf_x;
INSERT INTO t_05619_src VALUES (6);
OPTIMIZE TABLE t_05619_buf_y;
INSERT INTO t_05619_src VALUES (7);
OPTIMIZE TABLE t_05619_buf_x;
INSERT INTO t_05619_src VALUES (8);
OPTIMIZE TABLE t_05619_buf_y;

-- Only the row of the last query is still buffered.
SELECT 'written before the last flush', count() FROM t_05619_dst;

OPTIMIZE TABLE t_05619_buf_x;
OPTIMIZE TABLE t_05619_buf_y;

SELECT 'written', count(), sum(n) FROM t_05619_dst;

DROP TABLE t_05619_mv_direct;
DROP TABLE t_05619_mv_x;
DROP TABLE t_05619_mv_y;
DROP TABLE t_05619_src;
DROP TABLE t_05619_buf_x;
DROP TABLE t_05619_buf_y;
DROP TABLE t_05619_dst;
