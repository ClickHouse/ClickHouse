-- A filter attached to the read of a table during planning (`additional_table_filters`) that reads the
-- whole column keeps it read, so the subcolumn is extracted from it instead of being read a second time.
-- A function-form carrier (`length(s)`) must stay literal when it would reach a column used by the
-- primary key or a projection through several subquery layers.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_tup;
DROP TABLE IF EXISTS t_pk;
DROP TABLE IF EXISTS t_proj;
DROP TABLE IF EXISTS t_where_proj;

CREATE TABLE t_tup (tup Tuple(a String, b UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_tup SELECT (repeat(hex(cityHash64(number)), 12), number % 255) FROM numbers(10000);

SELECT 'additional filter reads the whole column';
SELECT count() FROM (EXPLAIN actions = 1 SELECT sum(cityHash64(tup.a)) FROM (SELECT tup FROM t_tup)
    SETTINGS additional_table_filters = {'t_tup': 'cityHash64(tup) > 0'}) WHERE explain LIKE '%tup.a%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT sum(cityHash64(tup.a)) FROM (SELECT tup FROM t_tup)
    SETTINGS additional_table_filters = {'t_tup': 'cityHash64(t_tup.tup) > 0'}) WHERE explain LIKE '%tup.a%';
SELECT sum(cityHash64(tup.a)) = (SELECT sum(cityHash64(tup.a)) FROM t_tup WHERE cityHash64(tup) > 0)
FROM (SELECT tup FROM t_tup) SETTINGS additional_table_filters = {'t_tup': 'cityHash64(tup) > 0'};

SELECT 'additional filter reads another subcolumn';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT sum(cityHash64(tup.a)) FROM (SELECT tup FROM t_tup)
    SETTINGS additional_table_filters = {'t_tup': 'tup.b > 0'}) WHERE explain LIKE '%tup.a%';

CREATE TABLE t_pk (s String) ENGINE = MergeTree ORDER BY length(s) SETTINGS index_granularity = 128;
INSERT INTO t_pk SELECT repeat('a', number % 100) FROM numbers(100000);

SELECT 'primary key through two subquery layers';
SELECT count() FROM (SELECT s FROM (SELECT s FROM t_pk)) WHERE length(s) = 5 SETTINGS force_primary_key = 1;

CREATE TABLE t_proj (s String, PROJECTION p (SELECT length(s) AS l, count() AS c GROUP BY length(s)))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_proj SELECT repeat('a', number % 100) FROM numbers(100000);

SELECT 'aggregate projection through two subquery layers';
SELECT length(s) AS l, count() FROM (SELECT s FROM (SELECT s FROM t_proj)) GROUP BY l ORDER BY l LIMIT 3
SETTINGS force_optimize_projection = 1;

CREATE TABLE t_where_proj (id UInt64, s String, PROJECTION p (SELECT id, s WHERE length(s) > 98 ORDER BY id))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 128;
INSERT INTO t_where_proj SELECT number, repeat('a', number % 100) FROM numbers(100000);

SELECT 'filtered projection through two subquery layers';
SELECT count() FROM (SELECT s FROM (SELECT s FROM t_where_proj)) WHERE length(s) > 98 SETTINGS force_optimize_projection = 1;

DROP TABLE t_tup;
DROP TABLE t_pk;
DROP TABLE t_proj;
DROP TABLE t_where_proj;
