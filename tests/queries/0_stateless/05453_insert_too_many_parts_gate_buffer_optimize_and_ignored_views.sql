-- The `Too many parts` decision shared by the sinks of one INSERT writing into the same table is not
-- reused by a later write that the decision did not accept:
-- 1. A `Buffer` that keeps the rows of the query in memory and is flushed later by `OPTIMIZE` applies
--    its own limits to the flush, even if a sibling branch of the query writing into the destination
--    directly was accepted with looser ones.
-- 2. With `materialized_views_ignore_errors` the rejection of one view branch does not reject a sibling
--    branch writing into the same table with looser limits.

DROP TABLE IF EXISTS t_05453_mv_direct;
DROP TABLE IF EXISTS t_05453_mv_buf;
DROP TABLE IF EXISTS t_05453_src;
DROP TABLE IF EXISTS t_05453_buf;
DROP TABLE IF EXISTS t_05453_dst;

CREATE TABLE t_05453_dst (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 1;

-- Small blocks stay in memory.
CREATE TABLE t_05453_buf (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05453_dst, 1, 1000000, 1000000, 1000000, 1000000, 1000000000, 1000000000);

CREATE TABLE t_05453_src (n UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW t_05453_mv_direct TO t_05453_dst AS SELECT n FROM t_05453_src;
CREATE MATERIALIZED VIEW t_05453_mv_buf TO t_05453_buf AS SELECT n + 1000 AS n FROM t_05453_src;

INSERT INTO t_05453_dst VALUES (100);

-- The direct branch writes under the raised query-level limit, the buffer keeps its row.
INSERT INTO t_05453_src SETTINGS parts_to_throw_insert = 1000, max_insert_threads = 1, parallel_view_processing = 0 VALUES (1);
SELECT n FROM t_05453_dst ORDER BY n;

-- The flush writes with the limit of the table, which already has two parts.
OPTIMIZE TABLE t_05453_buf; -- { serverError TOO_MANY_PARTS }
SELECT count() FROM t_05453_dst WHERE n >= 1000;

DROP TABLE t_05453_mv_direct;
DROP TABLE t_05453_mv_buf;
DROP TABLE t_05453_src;
DROP TABLE t_05453_buf;
DROP TABLE t_05453_dst;

CREATE TABLE t_05453_dst (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 1000;

-- Every block exceeds the zero `max_rows` threshold and is written directly into the destination.
CREATE TABLE t_05453_buf (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05453_dst, 1, 1000000, 1000000, 1000000, 0, 1000000000, 1000000000);

CREATE TABLE t_05453_src (n UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW t_05453_mv_direct TO t_05453_dst AS SELECT n FROM t_05453_src;
CREATE MATERIALIZED VIEW t_05453_mv_buf TO t_05453_buf AS SELECT n + 1000 AS n FROM t_05453_src;

INSERT INTO t_05453_dst VALUES (100);

-- The direct branch is rejected by the query-level limit and its error is ignored; the branch through the
-- buffer writes with the limit of the table.
INSERT INTO t_05453_src SETTINGS parts_to_throw_insert = 1, materialized_views_ignore_errors = 1, parallel_view_processing = 0 VALUES (1);
INSERT INTO t_05453_src SETTINGS parts_to_throw_insert = 1, materialized_views_ignore_errors = 1, parallel_view_processing = 1 VALUES (2);
SELECT n FROM t_05453_dst ORDER BY n;

DROP TABLE t_05453_mv_direct;
DROP TABLE t_05453_mv_buf;
DROP TABLE t_05453_src;
DROP TABLE t_05453_buf;
DROP TABLE t_05453_dst;
