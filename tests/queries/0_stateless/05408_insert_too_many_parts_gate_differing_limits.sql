-- The sinks of one INSERT writing into the same table share one `Too many parts` decision, but they do
-- not necessarily check the same limits: the query-level `parts_to_throw_insert` applies to the
-- branches of the materialized views, while a write through a `Buffer` runs with the context of the
-- buffer. The decision must take the checks of all the sinks into account, whichever of them starts
-- first: a write through the buffer with looser limits used to accept the decision for the table, and
-- the stricter direct branch skipped its own check.

DROP TABLE IF EXISTS t_05408_mv_a;
DROP TABLE IF EXISTS t_05408_mv_b;
DROP TABLE IF EXISTS t_05408_mv_c;
DROP TABLE IF EXISTS t_05408_mv_d;
DROP TABLE IF EXISTS t_05408_src_1;
DROP TABLE IF EXISTS t_05408_src_2;
DROP TABLE IF EXISTS t_05408_buf;
DROP TABLE IF EXISTS t_05408_dst;

CREATE TABLE t_05408_dst (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 1000;

-- Every block exceeds the zero `max_rows` threshold and is written directly into the destination.
CREATE TABLE t_05408_buf (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05408_dst, 1, 1000000, 1000000, 1000000, 0, 1000000000, 1000000000);

CREATE TABLE t_05408_src_1 (n UInt64) ENGINE = Null;
CREATE TABLE t_05408_src_2 (n UInt64) ENGINE = Null;

-- Both orders of the branches: through the buffer first, and directly first.
CREATE MATERIALIZED VIEW t_05408_mv_a TO t_05408_buf AS SELECT n FROM t_05408_src_1;
CREATE MATERIALIZED VIEW t_05408_mv_b TO t_05408_dst AS SELECT n FROM t_05408_src_1;
CREATE MATERIALIZED VIEW t_05408_mv_c TO t_05408_dst AS SELECT n FROM t_05408_src_2;
CREATE MATERIALIZED VIEW t_05408_mv_d TO t_05408_buf AS SELECT n FROM t_05408_src_2;

INSERT INTO t_05408_dst VALUES (100);

-- The direct branch is limited to one part by the query, and the table already has one.
INSERT INTO t_05408_src_1 SETTINGS parts_to_throw_insert = 1, parallel_view_processing = 0 VALUES (1); -- { serverError TOO_MANY_PARTS }
INSERT INTO t_05408_src_2 SETTINGS parts_to_throw_insert = 1, parallel_view_processing = 0 VALUES (2); -- { serverError TOO_MANY_PARTS }
INSERT INTO t_05408_src_1 SETTINGS parts_to_throw_insert = 1, parallel_view_processing = 1 VALUES (3); -- { serverError TOO_MANY_PARTS }
INSERT INTO t_05408_src_2 SETTINGS parts_to_throw_insert = 1, parallel_view_processing = 1 VALUES (4); -- { serverError TOO_MANY_PARTS }

SELECT n FROM t_05408_dst ORDER BY n;

-- Without the query-level limit both branches are accepted.
INSERT INTO t_05408_src_1 VALUES (5);
INSERT INTO t_05408_src_2 VALUES (6);

SELECT n FROM t_05408_dst ORDER BY n;

DROP TABLE t_05408_mv_a;
DROP TABLE t_05408_mv_b;
DROP TABLE t_05408_mv_c;
DROP TABLE t_05408_mv_d;
DROP TABLE t_05408_src_1;
DROP TABLE t_05408_src_2;
DROP TABLE t_05408_buf;
DROP TABLE t_05408_dst;
