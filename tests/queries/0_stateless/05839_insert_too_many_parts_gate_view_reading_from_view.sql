-- A materialized view that is inserted into directly only passes the rows to its target table: the views
-- reading from the target are run, but a view reading from the materialized view itself is not. So an INSERT
-- forwarded into such a view through an `Alias` or a `Buffer` must not run the `Too many parts` check of the
-- destination of a view that writes nothing: it has to behave exactly as the direct INSERT does.

DROP TABLE IF EXISTS t_05839_buf;
DROP TABLE IF EXISTS t_05839_alias;
DROP TABLE IF EXISTS t_05839_mv_from_target;
DROP TABLE IF EXISTS t_05839_mv_from_view;
DROP TABLE IF EXISTS t_05839_mv;
DROP TABLE IF EXISTS t_05839_src;
DROP TABLE IF EXISTS t_05839_ok;
DROP TABLE IF EXISTS t_05839_full;
DROP TABLE IF EXISTS t_05839_full_reached;

CREATE TABLE t_05839_src (n UInt64) ENGINE = MergeTree ORDER BY n;
CREATE TABLE t_05839_ok (n UInt64) ENGINE = MergeTree ORDER BY n;
CREATE TABLE t_05839_full (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS parts_to_throw_insert = 1;
INSERT INTO t_05839_full VALUES (0);

CREATE MATERIALIZED VIEW t_05839_mv TO t_05839_ok AS SELECT n FROM t_05839_src;
-- Not run by an INSERT into `t_05839_mv`: its destination is over the limit, but nothing is written into it.
CREATE MATERIALIZED VIEW t_05839_mv_from_view TO t_05839_full AS SELECT n FROM t_05839_mv;

CREATE TABLE t_05839_alias ENGINE = Alias('t_05839_mv');
-- Every block exceeds the zero max_rows threshold and is written directly, skipping the buffer.
CREATE TABLE t_05839_buf (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05839_mv, 1, 1000000, 1000000, 1000000, 0, 1000000000, 1000000000);

SELECT 'direct';
INSERT INTO t_05839_mv VALUES (1);
SELECT count() FROM t_05839_ok;
SELECT count() FROM t_05839_full;

SELECT 'alias';
INSERT INTO t_05839_alias VALUES (2);
SELECT count() FROM t_05839_ok;
SELECT count() FROM t_05839_full;

SELECT 'buffer';
INSERT INTO t_05839_buf VALUES (3);
SELECT count() FROM t_05839_ok;
SELECT count() FROM t_05839_full;

-- A view reading from the target of the materialized view is run by all the three INSERTs, so the limit of
-- its destination is still enforced, before anything is written.
CREATE TABLE t_05839_full_reached (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS parts_to_throw_insert = 1;
INSERT INTO t_05839_full_reached VALUES (0);
CREATE MATERIALIZED VIEW t_05839_mv_from_target TO t_05839_full_reached AS SELECT n FROM t_05839_ok;

SELECT 'view reading from the target';
INSERT INTO t_05839_mv VALUES (4); -- { serverError TOO_MANY_PARTS }
INSERT INTO t_05839_alias VALUES (5); -- { serverError TOO_MANY_PARTS }
INSERT INTO t_05839_buf VALUES (6); -- { serverError TOO_MANY_PARTS }
SELECT count() FROM t_05839_ok;
SELECT count() FROM t_05839_full_reached;

DROP TABLE t_05839_buf;
DROP TABLE t_05839_alias;
DROP TABLE t_05839_mv_from_target;
DROP TABLE t_05839_mv_from_view;
DROP TABLE t_05839_mv;
DROP TABLE t_05839_src;
DROP TABLE t_05839_ok;
DROP TABLE t_05839_full;
DROP TABLE t_05839_full_reached;
