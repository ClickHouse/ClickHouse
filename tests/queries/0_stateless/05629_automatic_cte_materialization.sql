-- A non-recursive CTE that aggregates and is referenced more than once is materialized automatically:
-- it is executed once, so both references see the same result of a non-deterministic subquery.

SET enable_analyzer = 1;
SET enable_materialized_cte = 0;

SELECT '-- aggregation, referenced twice: materialized';
WITH t AS (SELECT rand64() AS r, count() AS c FROM numbers(10) GROUP BY number)
SELECT count() FROM t AS a INNER JOIN t AS b ON a.r = b.r;

SELECT '-- the same with the setting disabled: inlined';
WITH t AS (SELECT rand64() AS r, count() AS c FROM numbers(10) GROUP BY number)
SELECT count() FROM t AS a INNER JOIN t AS b ON a.r = b.r
SETTINGS enable_automatic_cte_materialization = 0;

SELECT '-- NOT MATERIALIZED: inlined';
WITH t AS NOT MATERIALIZED (SELECT rand64() AS r, count() AS c FROM numbers(10) GROUP BY number)
SELECT count() FROM t AS a INNER JOIN t AS b ON a.r = b.r;

SELECT '-- no aggregation: inlined';
WITH t AS (SELECT rand64() AS r FROM numbers(10))
SELECT count() FROM t AS a INNER JOIN t AS b ON a.r = b.r;

SELECT '-- aggregate function without GROUP BY';
WITH t AS (SELECT max(rand64()) AS r FROM numbers(10))
SELECT count() FROM t AS a INNER JOIN t AS b ON a.r = b.r;

SELECT '-- DISTINCT';
WITH t AS (SELECT DISTINCT rand64() AS r FROM numbers(10))
SELECT count() FROM t AS a INNER JOIN t AS b ON a.r = b.r;

SELECT '-- window function';
WITH t AS (SELECT rand64() + 0 * row_number() OVER () AS r FROM numbers(10))
SELECT count() FROM t AS a INNER JOIN t AS b ON a.r = b.r;

SELECT '-- IN and JOIN references';
WITH t AS (SELECT rand64() AS r FROM numbers(10) GROUP BY number, r)
SELECT count() FROM t WHERE r IN (SELECT r FROM t);

SELECT '-- UNION ALL of references';
WITH t AS (SELECT sum(rand64() % 1000) AS r FROM numbers(1000))
SELECT uniqExact(r) FROM (SELECT r FROM t UNION ALL SELECT r FROM t UNION ALL SELECT r FROM t);

SELECT '-- referenced from a nested CTE';
WITH t AS (SELECT rand64() AS r FROM numbers(10) GROUP BY number, r), u AS (SELECT r FROM t)
SELECT count() FROM u AS a INNER JOIN t AS b ON a.r = b.r;

SELECT '-- the CTE body resolves differently at the two references: the results are kept';
WITH y AS (SELECT 1 AS x), t AS (SELECT x, count() AS c FROM y GROUP BY x)
SELECT * FROM t, (WITH y AS (SELECT 2 AS x) SELECT x AS x2, c AS c2 FROM t) AS s;

SELECT '-- correlated CTE: inlined, the result is kept';
SELECT number, (WITH t AS (SELECT count() AS c FROM numbers(10) WHERE number < n.number) SELECT sum(c) FROM (SELECT c FROM t UNION ALL SELECT c FROM t))
FROM numbers(3) AS n ORDER BY number
SETTINGS allow_experimental_correlated_subqueries = 1;

SELECT '-- recursive WITH: not materialized, the result is kept';
WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 5), s AS (SELECT sum(n) AS total FROM r)
SELECT a.total, b.total FROM s AS a, s AS b;

SELECT '-- results are the same as without materialization';
DROP TABLE IF EXISTS t_auto_cte;
CREATE TABLE t_auto_cte (id UInt64, page String, referer String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_auto_cte SELECT number, toString(number % 7), toString(number % 5) FROM numbers(1000);

WITH top_pages AS (SELECT page, count() AS c FROM t_auto_cte GROUP BY page ORDER BY c DESC, page LIMIT 3)
SELECT tp.page, tp.c, o.n
FROM top_pages AS tp
LEFT JOIN (SELECT referer, count() AS n FROM t_auto_cte WHERE referer IN (SELECT page FROM top_pages) GROUP BY referer) AS o
    ON o.referer = tp.page
ORDER BY tp.c DESC, tp.page;

WITH top_pages AS (SELECT page, count() AS c FROM t_auto_cte GROUP BY page ORDER BY c DESC, page LIMIT 3)
SELECT tp.page, tp.c, o.n
FROM top_pages AS tp
LEFT JOIN (SELECT referer, count() AS n FROM t_auto_cte WHERE referer IN (SELECT page FROM top_pages) GROUP BY referer) AS o
    ON o.referer = tp.page
ORDER BY tp.c DESC, tp.page
SETTINGS enable_automatic_cte_materialization = 0;

SELECT '-- the table is read twice instead of three times';
SELECT countIf(explain LIKE '%ReadFromMergeTree%') FROM (EXPLAIN
WITH top_pages AS (SELECT page, count() AS c FROM t_auto_cte GROUP BY page ORDER BY c DESC, page LIMIT 3)
SELECT tp.page, tp.c, o.n
FROM top_pages AS tp
LEFT JOIN (SELECT referer, count() AS n FROM t_auto_cte WHERE referer IN (SELECT page FROM top_pages) GROUP BY referer) AS o
    ON o.referer = tp.page);

SELECT countIf(explain LIKE '%ReadFromMergeTree%') FROM (EXPLAIN
WITH top_pages AS NOT MATERIALIZED (SELECT page, count() AS c FROM t_auto_cte GROUP BY page ORDER BY c DESC, page LIMIT 3)
SELECT tp.page, tp.c, o.n
FROM top_pages AS tp
LEFT JOIN (SELECT referer, count() AS n FROM t_auto_cte WHERE referer IN (SELECT page FROM top_pages) GROUP BY referer) AS o
    ON o.referer = tp.page);

DROP TABLE t_auto_cte;

SELECT '-- formatting';
SELECT formatQuerySingleLine('WITH t AS NOT MATERIALIZED (SELECT 1) SELECT * FROM t');
SELECT formatQuerySingleLine('WITH t AS MATERIALIZED (SELECT 1) SELECT * FROM t');
