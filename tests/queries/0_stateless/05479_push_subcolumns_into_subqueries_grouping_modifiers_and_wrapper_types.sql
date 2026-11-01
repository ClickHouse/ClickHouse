-- Keys of `ROLLUP`, `CUBE` and `GROUPING SETS` replaced with the same pushed column must not be merged,
-- and a subcolumn must not be forwarded through a wrapper whose column type differs from its destination.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_arr;
CREATE TABLE t_arr (arr Array(UInt32)) ENGINE = Memory;
INSERT INTO t_arr VALUES ([1]), ([1, 2]);

SELECT 'cube';
SELECT length(arr), arr.size0, grouping(length(arr), arr.size0), count()
FROM (SELECT arr FROM t_arr)
GROUP BY length(arr), arr.size0 WITH CUBE
ORDER BY ALL
SETTINGS optimize_push_subcolumns_into_subqueries = 1;

SELECT 'rollup';
SELECT length(arr), arr.size0, grouping(length(arr), arr.size0), count()
FROM (SELECT arr FROM t_arr)
GROUP BY length(arr), arr.size0 WITH ROLLUP
ORDER BY ALL
SETTINGS optimize_push_subcolumns_into_subqueries = 1;

SELECT 'grouping sets';
SELECT length(arr), arr.size0, grouping(length(arr), arr.size0), count()
FROM (SELECT arr FROM t_arr)
GROUP BY GROUPING SETS ((length(arr), arr.size0), (length(arr)), (arr.size0), ())
ORDER BY ALL
SETTINGS optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE t_arr;

DROP TABLE IF EXISTS buf;
DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS feed;
DROP TABLE IF EXISTS dst;

CREATE TABLE dst (n UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dst VALUES (42);

SELECT 'buffer';
CREATE TABLE buf (n Nullable(UInt8)) ENGINE = Buffer(currentDatabase(), dst, 1, 100, 1000, 1000000, 10000000, 1000000, 10000000);
SELECT count(n) FROM (SELECT n FROM buf) SETTINGS optimize_push_subcolumns_into_subqueries = 1;
SELECT n IS NULL FROM (SELECT n FROM buf) SETTINGS optimize_push_subcolumns_into_subqueries = 1;

SELECT 'materialized view';
CREATE TABLE feed (n UInt8) ENGINE = Null;
CREATE MATERIALIZED VIEW mv TO dst (n Nullable(UInt8)) AS SELECT toNullable(n) AS n FROM feed;
SELECT count(n) FROM (SELECT n FROM mv) SETTINGS optimize_push_subcolumns_into_subqueries = 1;
SELECT n IS NULL FROM (SELECT n FROM mv) SETTINGS optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE buf;
DROP TABLE mv;
DROP TABLE feed;
DROP TABLE dst;
