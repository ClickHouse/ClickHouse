-- The sinks of one INSERT writing into the same table share one `Too many parts` decision. A sink that
-- forwards the write through a nested INSERT creates the sinks behind it only during the execution, so a
-- sibling branch writing into the same table directly may have made the decision before they exist. The
-- check of the hidden write must still be enforced, whichever of the branches starts first, also when
-- the hidden write is made by a materialized view of the table behind the forwarding sink, or by the
-- local shard of a `Distributed` table.

DROP TABLE IF EXISTS t_05449_mv_a;
DROP TABLE IF EXISTS t_05449_mv_b;
DROP TABLE IF EXISTS t_05449_mv_c;
DROP TABLE IF EXISTS t_05449_mv_d;
DROP TABLE IF EXISTS t_05449_mv_e;
DROP TABLE IF EXISTS t_05449_mv_f;
DROP TABLE IF EXISTS t_05449_mv_g;
DROP TABLE IF EXISTS t_05449_mv_h;
DROP TABLE IF EXISTS t_05449_mv_mid;
DROP TABLE IF EXISTS t_05449_src_1;
DROP TABLE IF EXISTS t_05449_src_2;
DROP TABLE IF EXISTS t_05449_src_3;
DROP TABLE IF EXISTS t_05449_src_4;
DROP TABLE IF EXISTS t_05449_buf_mid;
DROP TABLE IF EXISTS t_05449_buf_dist;
DROP TABLE IF EXISTS t_05449_dist;
DROP TABLE IF EXISTS t_05449_mid;
DROP TABLE IF EXISTS t_05449_dst;

CREATE TABLE t_05449_dst (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 1;

-- A table without storage of its own: only its materialized view writes into the destination.
CREATE TABLE t_05449_mid (n UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW t_05449_mv_mid TO t_05449_dst AS SELECT n + 1000 AS n FROM t_05449_mid;

CREATE TABLE t_05449_dist (n UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05449_dst);

-- Every block exceeds the zero `max_rows` threshold and is written directly, with the settings of the buffer.
CREATE TABLE t_05449_buf_mid (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05449_mid, 1, 1000000, 1000000, 1000000, 0, 1000000000, 1000000000);
CREATE TABLE t_05449_buf_dist (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05449_dist, 1, 1000000, 1000000, 1000000, 0, 1000000000, 1000000000);

CREATE TABLE t_05449_src_1 (n UInt64) ENGINE = Null;
CREATE TABLE t_05449_src_2 (n UInt64) ENGINE = Null;
CREATE TABLE t_05449_src_3 (n UInt64) ENGINE = Null;
CREATE TABLE t_05449_src_4 (n UInt64) ENGINE = Null;

-- Both orders of the branches: through the buffer first, and directly first.
CREATE MATERIALIZED VIEW t_05449_mv_a TO t_05449_buf_mid AS SELECT n FROM t_05449_src_1;
CREATE MATERIALIZED VIEW t_05449_mv_b TO t_05449_dst AS SELECT n FROM t_05449_src_1;
CREATE MATERIALIZED VIEW t_05449_mv_c TO t_05449_dst AS SELECT n FROM t_05449_src_2;
CREATE MATERIALIZED VIEW t_05449_mv_d TO t_05449_buf_mid AS SELECT n FROM t_05449_src_2;

CREATE MATERIALIZED VIEW t_05449_mv_e TO t_05449_buf_dist AS SELECT n + 2000 AS n FROM t_05449_src_3;
CREATE MATERIALIZED VIEW t_05449_mv_f TO t_05449_dst AS SELECT n FROM t_05449_src_3;
CREATE MATERIALIZED VIEW t_05449_mv_g TO t_05449_dst AS SELECT n FROM t_05449_src_4;
CREATE MATERIALIZED VIEW t_05449_mv_h TO t_05449_buf_dist AS SELECT n + 2000 AS n FROM t_05449_src_4;

INSERT INTO t_05449_dst VALUES (100);

-- The query raises the limit for the direct branch, but the write through the buffer runs with the
-- limit of the table, and the table already has one part.
INSERT INTO t_05449_src_1 SETTINGS parts_to_throw_insert = 1000, parallel_view_processing = 0 VALUES (1); -- { serverError TOO_MANY_PARTS }
INSERT INTO t_05449_src_2 SETTINGS parts_to_throw_insert = 1000, parallel_view_processing = 0 VALUES (2); -- { serverError TOO_MANY_PARTS }
INSERT INTO t_05449_src_1 SETTINGS parts_to_throw_insert = 1000, parallel_view_processing = 1 VALUES (3); -- { serverError TOO_MANY_PARTS }
INSERT INTO t_05449_src_2 SETTINGS parts_to_throw_insert = 1000, parallel_view_processing = 1 VALUES (4); -- { serverError TOO_MANY_PARTS }

INSERT INTO t_05449_src_3 SETTINGS parts_to_throw_insert = 1000, parallel_view_processing = 0 VALUES (5); -- { serverError TOO_MANY_PARTS }
INSERT INTO t_05449_src_4 SETTINGS parts_to_throw_insert = 1000, parallel_view_processing = 0 VALUES (6); -- { serverError TOO_MANY_PARTS }
INSERT INTO t_05449_src_3 SETTINGS parts_to_throw_insert = 1000, parallel_view_processing = 1 VALUES (7); -- { serverError TOO_MANY_PARTS }
INSERT INTO t_05449_src_4 SETTINGS parts_to_throw_insert = 1000, parallel_view_processing = 1 VALUES (8); -- { serverError TOO_MANY_PARTS }

-- Nothing has been written through the buffers.
SELECT count() FROM t_05449_dst WHERE n >= 1000;

DROP TABLE t_05449_mv_a;
DROP TABLE t_05449_mv_b;
DROP TABLE t_05449_mv_c;
DROP TABLE t_05449_mv_d;
DROP TABLE t_05449_mv_e;
DROP TABLE t_05449_mv_f;
DROP TABLE t_05449_mv_g;
DROP TABLE t_05449_mv_h;
DROP TABLE t_05449_mv_mid;
DROP TABLE t_05449_src_1;
DROP TABLE t_05449_src_2;
DROP TABLE t_05449_src_3;
DROP TABLE t_05449_src_4;
DROP TABLE t_05449_buf_mid;
DROP TABLE t_05449_buf_dist;
DROP TABLE t_05449_dist;
DROP TABLE t_05449_mid;
DROP TABLE t_05449_dst;
