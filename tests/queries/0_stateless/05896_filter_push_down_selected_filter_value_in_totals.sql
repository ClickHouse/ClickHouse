-- the totals row is not filtered, so a selected filter expression keeps the value of the whole expression there

SELECT 'ARRAY JOIN';
SELECT a, s, s = 2 AND a = 10 AS f
FROM (SELECT sum(number) AS s FROM numbers(4) GROUP BY bitAnd(number, 1) WITH TOTALS)
ARRAY JOIN [10, 20] AS a
WHERE f
ORDER BY a, s;

SELECT 'DISTINCT';
SELECT s, s = 2 AND rand() >= 0 AS f
FROM (SELECT DISTINCT s FROM (SELECT sum(number) AS s FROM numbers(4) GROUP BY bitAnd(number, 1) WITH TOTALS))
WHERE f
ORDER BY s;

SELECT 'another expression reads the filter';
SELECT s, NOT (s = 2 AND rand() >= 0) AS g
FROM (SELECT DISTINCT s FROM (SELECT sum(number) AS s FROM numbers(4) GROUP BY bitAnd(number, 1) WITH TOTALS))
WHERE s = 2 AND rand() >= 0
ORDER BY s;

SELECT 'the filter is Bool';
SELECT s, (s = 2)::Bool AND (rand() >= 0)::Bool AS f
FROM (SELECT DISTINCT s FROM (SELECT sum(number) AS s FROM numbers(4) GROUP BY bitAnd(number, 1) WITH TOTALS))
WHERE f
ORDER BY s;

SELECT 'the part that stays above is a selected column';
SELECT a, s, s = 2 AND a AS f
FROM (SELECT sum(number) AS s FROM numbers(4) GROUP BY bitAnd(number, 1) WITH TOTALS)
ARRAY JOIN [0, 1] AS a
WHERE f
ORDER BY a, s
SETTINGS query_plan_fuse_filter_into_array_join = 0;

-- the part of the filter still goes below DISTINCT
SELECT 'plan';
SELECT count()
FROM
(
    EXPLAIN SELECT s, s = 2 AND rand() >= 0 AS f
    FROM (SELECT DISTINCT s FROM (SELECT sum(number) AS s FROM numbers(4) GROUP BY bitAnd(number, 1) WITH TOTALS))
    WHERE f
)
WHERE explain LIKE '%Filter column: sum(number) = 2';
