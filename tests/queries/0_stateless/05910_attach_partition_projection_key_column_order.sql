-- A projection defined with a column matcher expands over the columns in table order, so equal definitions
-- can have different sorting keys; partition clones between such tables must be refused.

DROP TABLE IF EXISTS t_dst;
DROP TABLE IF EXISTS t_src;

CREATE TABLE t_dst (id UInt64, x UInt64, y UInt64, PROJECTION p INDEX * TYPE basic) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_src (id UInt64, y UInt64, x UInt64, PROJECTION p INDEX * TYPE basic) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_src SELECT number, number, number % 100 FROM numbers(1000);
INSERT INTO t_dst SELECT number, number % 100, number FROM numbers(1000);

SELECT table, sorting_key FROM system.projections WHERE database = currentDatabase() AND table IN ('t_dst', 't_src') ORDER BY table;

ALTER TABLE t_dst ATTACH PARTITION tuple() FROM t_src; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_dst REPLACE PARTITION tuple() FROM t_src; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_src MOVE PARTITION tuple() TO TABLE t_dst; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM t_dst WHERE y >= 900 SETTINGS optimize_use_projections = 0;
SELECT count() FROM t_dst WHERE y >= 900 SETTINGS optimize_use_projections = 1;
SELECT count() FROM t_src;

DROP TABLE t_dst;
DROP TABLE t_src;

-- Other matcher forms of a projection key.
CREATE TABLE t_tuple_dst (id UInt64, x UInt64, y UInt64, PROJECTION p (SELECT id ORDER BY tuple(*))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_tuple_src (id UInt64, y UInt64, x UInt64, PROJECTION p (SELECT id ORDER BY tuple(*))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_tuple_src VALUES (1, 2, 3);
ALTER TABLE t_tuple_dst ATTACH PARTITION tuple() FROM t_tuple_src; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_tuple_dst;
DROP TABLE t_tuple_src;

CREATE TABLE t_columns_dst (id UInt64, x UInt64, y UInt64, PROJECTION p (SELECT id ORDER BY COLUMNS('x|y'))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_columns_src (id UInt64, y UInt64, x UInt64, PROJECTION p (SELECT id ORDER BY COLUMNS('x|y'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_columns_src VALUES (1, 2, 3);
ALTER TABLE t_columns_dst ATTACH PARTITION tuple() FROM t_columns_src; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_columns_dst;
DROP TABLE t_columns_src;

CREATE TABLE t_agg_dst (id UInt64, x UInt64, y UInt64, PROJECTION p (SELECT COLUMNS('x|y'), count() GROUP BY COLUMNS('x|y'))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_agg_src (id UInt64, y UInt64, x UInt64, PROJECTION p (SELECT COLUMNS('x|y'), count() GROUP BY COLUMNS('x|y'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_agg_src VALUES (1, 2, 3);
ALTER TABLE t_agg_dst ATTACH PARTITION tuple() FROM t_agg_src; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_agg_dst;
DROP TABLE t_agg_src;

-- Different column order with the same projection key is still accepted.
CREATE TABLE t_ok_dst (id UInt64, x UInt64, y UInt64, PROJECTION p (SELECT * ORDER BY x)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_ok_src (id UInt64, y UInt64, x UInt64, PROJECTION p (SELECT * ORDER BY x)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_ok_src SELECT number, number, number % 100 FROM numbers(1000);
INSERT INTO t_ok_dst SELECT number, number % 100, number FROM numbers(1000);
ALTER TABLE t_ok_dst ATTACH PARTITION tuple() FROM t_ok_src;
SELECT count(), sum(y) FROM t_ok_dst WHERE x = 5 SETTINGS optimize_use_projections = 0;
SELECT count(), sum(y) FROM t_ok_dst WHERE x = 5 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
OPTIMIZE TABLE t_ok_dst FINAL;
SELECT count(), sum(y) FROM t_ok_dst WHERE x = 5 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
DROP TABLE t_ok_dst;
DROP TABLE t_ok_src;

CREATE TABLE t_index_dst (id UInt64, x UInt64, y UInt64, PROJECTION p INDEX * TYPE basic) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_index_src (x UInt64, id UInt64, y UInt64, PROJECTION p INDEX * TYPE basic) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_index_src SELECT number % 100, number, number FROM numbers(1000);
INSERT INTO t_index_dst SELECT number, number % 100, number FROM numbers(1000);
ALTER TABLE t_index_dst ATTACH PARTITION tuple() FROM t_index_src;
SELECT count() FROM t_index_dst WHERE y >= 900 SETTINGS optimize_use_projections = 0;
SELECT count() FROM t_index_dst WHERE y >= 900 SETTINGS optimize_use_projections = 1;
DROP TABLE t_index_dst;
DROP TABLE t_index_src;
