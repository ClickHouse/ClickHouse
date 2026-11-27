-- The sinks of one INSERT writing into the same table share one `Too many parts` decision. A `Buffer`
-- creates the sink of its destination only when it writes a block directly, during the execution, so a
-- sibling branch writing into the destination directly may have made the decision before that sink
-- exists. The check of the write through the buffer, which runs with the limits of the buffer's context,
-- must still be enforced, whichever of the branches starts first. Rows that the buffer only keeps in
-- memory are not subject to the limits of the destination.

DROP TABLE IF EXISTS t_05448_mv_a;
DROP TABLE IF EXISTS t_05448_mv_b;
DROP TABLE IF EXISTS t_05448_mv_c;
DROP TABLE IF EXISTS t_05448_mv_d;
DROP TABLE IF EXISTS t_05448_mv_e;
DROP TABLE IF EXISTS t_05448_mv_f;
DROP TABLE IF EXISTS t_05448_src_1;
DROP TABLE IF EXISTS t_05448_src_2;
DROP TABLE IF EXISTS t_05448_src_3;
DROP TABLE IF EXISTS t_05448_buf_direct;
DROP TABLE IF EXISTS t_05448_buf_memory;
DROP TABLE IF EXISTS t_05448_dst;

CREATE TABLE t_05448_dst (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 1;

-- Every block exceeds the zero `max_rows` threshold and is written directly into the destination.
CREATE TABLE t_05448_buf_direct (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05448_dst, 1, 1000000, 1000000, 1000000, 0, 1000000000, 1000000000);

-- Small blocks stay in memory.
CREATE TABLE t_05448_buf_memory (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05448_dst, 1, 1000000, 1000000, 1000000, 1000000, 1000000000, 1000000000);

CREATE TABLE t_05448_src_1 (n UInt64) ENGINE = Null;
CREATE TABLE t_05448_src_2 (n UInt64) ENGINE = Null;
CREATE TABLE t_05448_src_3 (n UInt64) ENGINE = Null;

-- Both orders of the branches: through the buffer first, and directly first. The rows written through
-- the buffer are shifted, to tell them apart.
CREATE MATERIALIZED VIEW t_05448_mv_a TO t_05448_buf_direct AS SELECT n + 1000 AS n FROM t_05448_src_1;
CREATE MATERIALIZED VIEW t_05448_mv_b TO t_05448_dst AS SELECT n FROM t_05448_src_1;
CREATE MATERIALIZED VIEW t_05448_mv_c TO t_05448_dst AS SELECT n FROM t_05448_src_2;
CREATE MATERIALIZED VIEW t_05448_mv_d TO t_05448_buf_direct AS SELECT n + 1000 AS n FROM t_05448_src_2;

CREATE MATERIALIZED VIEW t_05448_mv_e TO t_05448_dst AS SELECT n FROM t_05448_src_3;
CREATE MATERIALIZED VIEW t_05448_mv_f TO t_05448_buf_memory AS SELECT n + 1000 AS n FROM t_05448_src_3;

INSERT INTO t_05448_dst VALUES (100);

-- The query raises the limit for the direct branch, but the write through the buffer runs with the
-- limit of the table, and the table already has one part.
INSERT INTO t_05448_src_1 SETTINGS parts_to_throw_insert = 1000, parallel_view_processing = 0 VALUES (1); -- { serverError TOO_MANY_PARTS }
INSERT INTO t_05448_src_2 SETTINGS parts_to_throw_insert = 1000, parallel_view_processing = 0 VALUES (2); -- { serverError TOO_MANY_PARTS }
INSERT INTO t_05448_src_1 SETTINGS parts_to_throw_insert = 1000, parallel_view_processing = 1 VALUES (3); -- { serverError TOO_MANY_PARTS }
INSERT INTO t_05448_src_2 SETTINGS parts_to_throw_insert = 1000, parallel_view_processing = 1 VALUES (4); -- { serverError TOO_MANY_PARTS }

-- Nothing has been written through the buffer.
SELECT count() FROM t_05448_dst WHERE n >= 1000;

-- The buffer keeps the rows in memory, so only the limit of the direct branch applies.
INSERT INTO t_05448_src_3 SETTINGS parts_to_throw_insert = 1000, parallel_view_processing = 0 VALUES (5);
INSERT INTO t_05448_src_3 SETTINGS parts_to_throw_insert = 1000, parallel_view_processing = 1 VALUES (6);

SELECT n FROM t_05448_dst WHERE n IN (5, 6) ORDER BY n;
SELECT n FROM t_05448_buf_memory WHERE n >= 1000 ORDER BY n;

DROP TABLE t_05448_mv_a;
DROP TABLE t_05448_mv_b;
DROP TABLE t_05448_mv_c;
DROP TABLE t_05448_mv_d;
DROP TABLE t_05448_mv_e;
DROP TABLE t_05448_mv_f;
DROP TABLE t_05448_src_1;
DROP TABLE t_05448_src_2;
DROP TABLE t_05448_src_3;
DROP TABLE t_05448_buf_direct;
DROP TABLE t_05448_buf_memory;
DROP TABLE t_05448_dst;
