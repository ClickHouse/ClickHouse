-- PASTE JOIN pairs rows by position, so ORDER BY in its inputs must survive removal of redundant sorting.
-- https://github.com/ClickHouse/ClickHouse/issues/72693

SET max_threads = 1;

-- Aggregation on top of PASTE JOIN: 4*0 + 3*1 + 2*2 + 1*3 + 0*4 = 10
SELECT sum(a * b)
FROM (SELECT number AS a FROM numbers(5) ORDER BY a DESC) AS t1
PASTE JOIN (SELECT number AS b FROM numbers(5) ORDER BY b) AS t2
SETTINGS query_plan_remove_redundant_sorting = 1;

SELECT sum(a * b)
FROM (SELECT number AS a FROM numbers(5) ORDER BY a DESC) AS t1
PASTE JOIN (SELECT number AS b FROM numbers(5) ORDER BY b) AS t2
SETTINGS query_plan_remove_redundant_sorting = 0;

-- Outer ORDER BY on top of PASTE JOIN must not change the pairing
SELECT a, b
FROM (SELECT number AS a FROM numbers(5) ORDER BY a DESC) AS t1
PASTE JOIN (SELECT number AS b FROM numbers(5) ORDER BY b) AS t2
ORDER BY a
SETTINGS query_plan_remove_redundant_sorting = 1;

SELECT a, b
FROM (SELECT number AS a FROM numbers(5) ORDER BY a DESC) AS t1
PASTE JOIN (SELECT number AS b FROM numbers(5) ORDER BY b) AS t2
ORDER BY a
SETTINGS query_plan_remove_redundant_sorting = 0;

-- Data from the issue: pairs (1,3) (2,3) (3,3) (3,4) (3,5) (4,9), sum of distances = 11
DROP TABLE IF EXISTS left_list;
DROP TABLE IF EXISTS right_list;
CREATE TABLE left_list (x Int32) ENGINE = Memory;
CREATE TABLE right_list (y Int32) ENGINE = Memory;
INSERT INTO left_list VALUES (3), (4), (2), (1), (3), (3);
INSERT INTO right_list VALUES (4), (3), (5), (3), (9), (3);

SELECT sum(abs(x - y))
FROM (SELECT x FROM left_list ORDER BY x) AS l
PASTE JOIN (SELECT y FROM right_list ORDER BY y) AS r
SETTINGS query_plan_remove_redundant_sorting = 1;

SELECT sum(abs(x - y))
FROM (SELECT x FROM left_list ORDER BY x) AS l
PASTE JOIN (SELECT y FROM right_list ORDER BY y) AS r
SETTINGS query_plan_remove_redundant_sorting = 0;

DROP TABLE left_list;
DROP TABLE right_list;

-- Sorting below PASTE JOIN is kept, while it is still removed below other joins
SET query_plan_enable_optimizations = 1, query_plan_remove_redundant_sorting = 1, join_algorithm = 'hash';

SELECT count()
FROM
(
    EXPLAIN
    SELECT sum(a * b)
    FROM (SELECT number AS a FROM numbers(5) ORDER BY a DESC) AS t1
    PASTE JOIN (SELECT number AS b FROM numbers(5) ORDER BY b) AS t2
)
WHERE explain LIKE '%Sorting%';

SELECT count()
FROM
(
    EXPLAIN
    SELECT sum(a * b)
    FROM (SELECT number AS a FROM numbers(5) ORDER BY a DESC) AS t1
    INNER JOIN (SELECT number AS b FROM numbers(5) ORDER BY b) AS t2 ON a = b
)
WHERE explain LIKE '%Sorting%';
