-- a projection with `WHERE` is used when its `SELECT` has an alias

SET optimize_read_in_order = 0;

DROP TABLE IF EXISTS t_filtered_projection_key_alias;
DROP TABLE IF EXISTS t_filtered_projection_where_alias;
DROP TABLE IF EXISTS t_filtered_projection_normal_alias;

CREATE TABLE t_filtered_projection_key_alias (a UInt8, b UInt8, PROJECTION p (SELECT a % 2 AS k, count() WHERE b = 1 GROUP BY k))
ENGINE = MergeTree ORDER BY a;
INSERT INTO t_filtered_projection_key_alias SELECT number, number % 3 FROM numbers(100);

SELECT a % 2 AS k, count() FROM t_filtered_projection_key_alias WHERE b = 1 GROUP BY k ORDER BY k
SETTINGS force_optimize_projection = 1;

-- `WHERE` refers to the alias
CREATE TABLE t_filtered_projection_where_alias (a UInt8, b UInt8, PROJECTION p (SELECT a % 2 AS k, count() WHERE k = 1 GROUP BY k))
ENGINE = MergeTree ORDER BY a;
INSERT INTO t_filtered_projection_where_alias SELECT number, number % 3 FROM numbers(100);

SELECT a % 2 AS k, count() FROM t_filtered_projection_where_alias WHERE a % 2 = 1 GROUP BY k ORDER BY k
SETTINGS force_optimize_projection = 1;

CREATE TABLE t_filtered_projection_normal_alias (a UInt8, b UInt8, c UInt8, PROJECTION p (SELECT a, b, c AS d WHERE b = 1 ORDER BY c))
ENGINE = MergeTree ORDER BY a;
INSERT INTO t_filtered_projection_normal_alias SELECT number, number % 3, number % 7 FROM numbers(100);

SELECT a FROM t_filtered_projection_normal_alias WHERE b = 1 AND c = 2 ORDER BY a
SETTINGS force_optimize_projection = 1;

DROP TABLE t_filtered_projection_key_alias;
DROP TABLE t_filtered_projection_where_alias;
DROP TABLE t_filtered_projection_normal_alias;
