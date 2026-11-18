-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN indexes = 1 shows the key condition of a local read only

SET query_plan_filter_push_down = 1;

DROP TABLE IF EXISTS t_rollup_filter;
CREATE TABLE t_rollup_filter (id UInt32, k UInt32, s String, v UInt64) ENGINE = MergeTree ORDER BY (id, k, s);
-- the real groups id = 0, k = 0 and s = '' have the same key value as the subtotal rows
INSERT INTO t_rollup_filter SELECT number % 10, number % 3, if(number % 4 = 0, '', 'a'), number FROM numbers(60);

SELECT 'rollup, first key';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY ROLLUP(id, k)) WHERE id = 4) WHERE explain LIKE '%Condition:%';
SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY ROLLUP(id, k)) WHERE id = 4 ORDER BY ALL;

SELECT 'cube, first key';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY CUBE(id, k)) WHERE id = 4) WHERE explain LIKE '%Condition:%';
SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY CUBE(id, k)) WHERE id = 4 ORDER BY ALL;

SELECT 'cube, second key';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY CUBE(id, k)) WHERE k = 2) WHERE explain LIKE '%Condition:%';
SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY CUBE(id, k)) WHERE k = 2 ORDER BY ALL;

SELECT 'range';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY ROLLUP(id, k)) WHERE id BETWEEN 3 AND 4) WHERE explain LIKE '%Condition:%';
SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY ROLLUP(id, k)) WHERE id BETWEEN 3 AND 4 ORDER BY ALL;

SELECT 'string key';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM (SELECT s, id, sum(v) FROM t_rollup_filter GROUP BY ROLLUP(s, id)) WHERE s = 'a' AND id = 4) WHERE explain LIKE '%Condition:%';
SELECT * FROM (SELECT s, id, sum(v) FROM t_rollup_filter GROUP BY ROLLUP(s, id)) WHERE s = 'a' AND id = 4 ORDER BY ALL;

SELECT 'having with grouping';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id, k, grouping(id, k) AS g, sum(v) FROM t_rollup_filter GROUP BY ROLLUP(id, k) HAVING id = 4 AND g = 1) WHERE explain LIKE '%Condition:%';
SELECT id, k, grouping(id, k) AS g, sum(v) FROM t_rollup_filter GROUP BY ROLLUP(id, k) HAVING id = 4 AND g = 1 ORDER BY ALL;

SELECT 'disjunction with an aggregate';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM (SELECT id, k, sum(v) AS sv FROM t_rollup_filter GROUP BY ROLLUP(id, k)) WHERE (id = 4 AND sv > 100) OR (id = 5 AND sv > 150)) WHERE explain LIKE '%Condition:%';
SELECT * FROM (SELECT id, k, sum(v) AS sv FROM t_rollup_filter GROUP BY ROLLUP(id, k)) WHERE (id = 4 AND sv > 100) OR (id = 5 AND sv > 150) ORDER BY ALL;

SELECT 'having, then where';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY ROLLUP(id, k) HAVING k = 2) WHERE id = 4) WHERE explain LIKE '%Condition:%';
SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY ROLLUP(id, k) HAVING k = 2) WHERE id = 4 ORDER BY ALL;

SELECT 'the condition has the name of a column below the step';
SELECT id % 3 AS a, count() FROM t_rollup_filter GROUP BY a WITH ROLLUP HAVING a ORDER BY ALL;
SELECT * FROM (SELECT id, k, count() FROM t_rollup_filter GROUP BY ROLLUP(id, k) HAVING k = 1) WHERE id ORDER BY ALL LIMIT 3;
SELECT * FROM (SELECT id % 5 > 2 AS f, id % 5 AS a, count() FROM t_rollup_filter GROUP BY f, a WITH CUBE) WHERE a > 2 ORDER BY ALL;
SELECT * FROM (SELECT id = 1 AND k = 1 AS f, id, k, count() FROM t_rollup_filter GROUP BY f, id, k WITH ROLLUP) WHERE id = 1 AND k = 1 ORDER BY ALL;

SELECT 'only the conjunct that rejects the subtotal value';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY ROLLUP(id, k)) WHERE id = 4 AND k != 1) WHERE explain LIKE '%Condition:%';
SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY ROLLUP(id, k)) WHERE id = 4 AND k != 1 ORDER BY ALL;

SELECT 'with totals';
SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY id, k WITH ROLLUP WITH TOTALS) WHERE id = 4 ORDER BY ALL;

SELECT 'not pushed: the filter keeps the subtotal value';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY ROLLUP(id, k)) WHERE id = 0) WHERE explain LIKE '%Condition:%';
SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY ROLLUP(id, k)) WHERE id = 0 ORDER BY ALL;
SELECT * FROM (SELECT s, id, sum(v) FROM t_rollup_filter GROUP BY CUBE(s, id)) WHERE s = '' AND id < 2 ORDER BY ALL;

SELECT 'not pushed: two keys in one conjunct';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY ROLLUP(id, k)) WHERE id + k = 5) WHERE explain LIKE '%Condition:%';
SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY ROLLUP(id, k)) WHERE id + k = 5 ORDER BY ALL;

SELECT 'not pushed: group_by_use_nulls';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY ROLLUP(id, k)) WHERE id = 4) WHERE explain LIKE '%Condition:%' SETTINGS group_by_use_nulls = 1;
SELECT * FROM (SELECT id, k, sum(v) FROM t_rollup_filter GROUP BY ROLLUP(id, k)) WHERE id = 4 ORDER BY ALL SETTINGS group_by_use_nulls = 1;

DROP TABLE t_rollup_filter;
