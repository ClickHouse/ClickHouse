-- The query of `view`, `viewIfPermitted` and `eval` is qualified with the current database before it is read as written.

SET enable_materialized_cte = 1;
SET enable_eval_table_function = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (x Int64) ENGINE = Memory;
INSERT INTO t VALUES (1);
CREATE TEMPORARY TABLE tmp_view_arg (z Int64);
INSERT INTO tmp_view_arg VALUES (7);

SELECT '-- 1. a table of the current database';
SELECT * FROM view(SELECT x FROM t);
SELECT * FROM viewIfPermitted(SELECT x FROM t ELSE null('x Int64'));
SELECT * FROM eval('SELECT x FROM t');

SELECT '-- 2. a common table expression is not a table';
SELECT * FROM view(WITH c AS (SELECT x + 10 AS y FROM t) SELECT y FROM c);
SELECT * FROM view(WITH c AS (SELECT x + 10 AS y FROM t) SELECT a.y FROM c AS a JOIN c AS b ON a.y = b.y);
SELECT * FROM eval('WITH c AS (SELECT x + 10 AS y FROM t) SELECT y FROM c');

SELECT '-- 3. recursive and materialized ones';
SELECT * FROM view(WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT max(n) FROM r);
SELECT * FROM view(WITH c AS MATERIALIZED (SELECT x + 20 AS y FROM t) SELECT y FROM c);

SELECT '-- 4. a temporary table';
SELECT * FROM view(SELECT z FROM tmp_view_arg);

SELECT '-- 5. a table on the right of IN and in a subquery';
SELECT * FROM view(SELECT 1 AS a WHERE 1 IN t);
SELECT * FROM view(SELECT (SELECT max(x) FROM t) AS m);

SELECT '-- 6. a nested view and a join';
SELECT * FROM view(SELECT * FROM view(SELECT x FROM t));
SELECT * FROM view(SELECT a.x + b.x AS s FROM t AS a JOIN t AS b ON a.x = b.x);

SELECT '-- 7. a table qualified with its database';
SELECT * FROM view(SELECT count() FROM system.one);

SELECT '-- 8. the query sent to a remote server is not changed';
SELECT * FROM remote('127.0.0.1', view(SELECT x FROM system.one AS o, (SELECT 5 AS x) AS s));

DROP TABLE t;
